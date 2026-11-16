"""采集站同步引擎的本地演练测试（不依赖 MySQL/Redis/NumPy）。

运行：
    cd backend && python -m tests.test_sync_engine
覆盖：增量同步、乱序/重复不倒退、撤回不被旧消息复活、游标过期全量副本+期间
增量+原子切换、中断恢复、同进度重试不新增批次、并发同站不跳过变更、缺口保留
旧批次、已发布批次不可变且分析仍绑定原批次。
"""
import os
import tempfile
import threading
import unittest

from sqlalchemy import create_engine, event

_TMP_DB = os.path.join(tempfile.gettempdir(), 'sync_drill_test.db')
if os.path.exists(_TMP_DB):
    os.remove(_TMP_DB)
os.environ.setdefault('DATABASE_URL', f'sqlite:///{_TMP_DB}')

from src.config import database as db_config

# SQLite 演练库：允许跨线程连接，开启 WAL 与忙等，模拟真实数据库的行锁语义
_test_engine = create_engine(
    os.environ['DATABASE_URL'],
    connect_args={'check_same_thread': False, 'timeout': 30})


@event.listens_for(_test_engine, 'connect')
def _sqlite_pragmas(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('PRAGMA busy_timeout=30000')
    cur.close()


db_config.engine = _test_engine
db_config.Session.configure(bind=_test_engine)

from sqlalchemy import select, func

from src.config.database import Base, Session
from src.models import (SyncStation, SourceEvent, SourceRecord, SyncRecordState,
                        SyncRun, SyncStaging, Dataset, Job)
from src.services import source_simulator as source
from src.services import sync_engine
from src.services.business import seed_records

Base.metadata.create_all(_test_engine)

COLUMNS = ['operator', 'team', 'before', 'after']


def row(key, before=400.0, after=370.0):
    return {'operator': key, 'team': '一组', 'before': before, 'after': after}


def make_station(db, code='ST-01', name='北岭一号采集站'):
    return source.create_station(db, code, name)


def count(db, model):
    return db.scalar(select(func.count()).select_from(model))


class SyncEngineTests(unittest.TestCase):
    def setUp(self):
        # 内存库每个连接独立，这里用单 Session 顺序执行，引擎内部提交不丢数据
        self.db = Session()

    def tearDown(self):
        self.db.rollback()
        for model in (SyncStaging, SyncRun, SyncRecordState, SourceRecord,
                      SourceEvent, SyncStation, Dataset):
            self.db.execute(model.__table__.delete())
        self.db.commit()
        self.db.close()

    # 1. 首次增量：发布批次，游标推进，逐记录版本保存
    def test_01_first_incremental_publishes_batch(self):
        db = self.db
        st = make_station(db)
        for i in range(1, 6):
            source.append_event(db, st, 'upsert', f'OP-{i:03}', row(f'OP-{i:03}'))
        db.commit()
        run = sync_engine.synchronize(db, st, owner='tester')
        self.assertEqual(run.status, 'published')
        self.assertEqual(run.applied_version, 5)
        self.assertEqual(st.last_cursor_version, 5)
        ds = db.get(Dataset, run.published_dataset_id)
        self.assertEqual(len(ds.records), 5)
        self.assertEqual(ds.source, f'sync://{st.code}#v5')
        self.assertEqual(count(db, SyncRecordState), 5)
        # 暂存区只服务全量副本，增量路径不应留下半成品
        self.assertEqual(count(db, SyncStaging), 0)

    # 2. 重复或乱序记录不倒退；相同进度重试不新增批次
    def test_02_retry_is_noop_and_duplicate_out_of_order_no_regression(self):
        db = self.db
        st = make_station(db)
        source.append_event(db, st, 'upsert', 'OP-001', row('OP-001', 400, 360))
        source.append_event(db, st, 'upsert', 'OP-002', row('OP-002', 410, 380))
        db.commit()
        first = sync_engine.synchronize(db, st)
        ds_before = first.published_dataset_id

        # 无新事件重试：caught_up，不新增批次
        again = sync_engine.synchronize(db, st)
        self.assertEqual(again.status, 'caught_up')
        self.assertIsNone(again.published_dataset_id)
        self.assertEqual(count(db, Dataset), 1)
        self.assertEqual(st.last_cursor_version, 2)

        # 源端补发一条低版本内容不会发生（版本单调），本地手工构造乱序投递：
        # 直接调用 _apply_event，旧版本事件必须被跳过
        stale = SourceEvent(station_id=st.id, version=1, op='upsert',
                            record_key='OP-001', payload=row('OP-001', 999, 1),
                            created_at=0)
        probe = SyncRun(station_id=st.id, mode='incremental', status='running',
                        created_at=0, updated_at=0)
        db.add(probe); db.flush()
        changed = sync_engine._apply_event(db, st.id, stale, probe)
        self.assertFalse(changed)
        self.assertEqual(probe.skipped_events, 1)
        state = db.scalar(select(SyncRecordState).where(
            SyncRecordState.record_key == 'OP-001'))
        self.assertEqual(state.version, 1)
        self.assertEqual(state.payload['before'], 400)

        # 新版本更新正常生效，并发布第二个批次；第一个批次内容不变
        source.append_event(db, st, 'upsert', 'OP-001', row('OP-001', 405, 355))
        db.commit()
        third = sync_engine.synchronize(db, st)
        self.assertEqual(third.status, 'published')
        self.assertNotEqual(third.published_dataset_id, ds_before)
        old = db.get(Dataset, ds_before)
        self.assertEqual(old.records[0]['before'], 400)
        new = db.get(Dataset, third.published_dataset_id)
        self.assertEqual(new.records[0]['before'], 405)

    # 3. 撤回写墓碑，旧消息不能复活撤回记录
    def test_03_withdraw_tombstone_blocks_stale_resurrection(self):
        db = self.db
        st = make_station(db)
        source.append_event(db, st, 'upsert', 'OP-001', row('OP-001'))
        source.append_event(db, st, 'upsert', 'OP-002', row('OP-002'))
        source.append_event(db, st, 'withdraw', 'OP-001', {})
        db.commit()
        run = sync_engine.synchronize(db, st)
        ds = db.get(Dataset, run.published_dataset_id)
        self.assertEqual(len(ds.records), 1)
        self.assertEqual(ds.records[0]['operator'], 'OP-002')
        tomb = db.scalar(select(SyncRecordState).where(
            SyncRecordState.record_key == 'OP-001'))
        self.assertEqual(tomb.op, 'withdraw')

        # 一条迟到的、版本更低的 upsert 试图复活
        stale = SourceEvent(station_id=st.id, version=1, op='upsert',
                            record_key='OP-001', payload=row('OP-001'), created_at=0)
        probe = SyncRun(station_id=st.id, mode='incremental', status='running',
                        created_at=0, updated_at=0)
        db.add(probe); db.flush()
        self.assertFalse(sync_engine._apply_event(db, st.id, stale, probe))
        tomb = db.scalar(select(SyncRecordState).where(
            SyncRecordState.record_key == 'OP-001'))
        self.assertEqual(tomb.op, 'withdraw')

        # 更高版本的重新录入可以恢复（同一自然键的新事实）
        source.append_event(db, st, 'upsert', 'OP-001', row('OP-001', 401, 361))
        db.commit()
        run2 = sync_engine.synchronize(db, st)
        ds2 = db.get(Dataset, run2.published_dataset_id)
        self.assertEqual(len(ds2.records), 2)

    # 4. 游标过期：全量副本 → 期间增量（含撤回）→ 一次性切换，半成品不可见
    def test_04_expired_cursor_full_copy_then_catch_up(self):
        db = self.db
        st = make_station(db)
        for i in range(1, 7):
            source.append_event(db, st, 'upsert', f'OP-{i:03}', row(f'OP-{i:03}'))
        db.commit()
        first = sync_engine.synchronize(db, st)
        self.assertEqual(first.status, 'published')

        # 源端推进保留窗口到本地游标之后（严格大于即过期），模拟日志截断
        source.advance_cursor(db, st, retain_after_version=7)
        db.commit()
        self.assertTrue(source.is_cursor_expired(st))

        # 全量复制进行期间源端继续产生新增与撤回，都必须纳入
        sync_engine.inject_fault('full_stage')
        # 复制开始前先制造“期间”变更：新增 OP-007、撤回 OP-002
        source.append_event(db, st, 'upsert', 'OP-007', row('OP-007'))
        source.append_event(db, st, 'withdraw', 'OP-002', {})
        db.commit()

        try:
            sync_engine.synchronize(db, st)
        except RuntimeError as exc:
            self.assertIn('full_stage', str(exc))
        db.rollback()
        # 故障只表示进程中断：恢复后源端事件日志仍然完整（保留窗口未再推进）
        self.assertEqual(st.retain_after_version, 7)
        # 中断后存在暂存半成品，但对分析侧不可见（未发布任何新 Dataset）
        self.assertEqual(count(db, Dataset), 1)
        interrupted = db.scalar(select(SyncRun).where(SyncRun.status == 'interrupted'))
        self.assertIsNotNone(interrupted)
        staging_rows = count(db, SyncStaging)
        self.assertGreater(staging_rows, 0)
        # 游标与已确认版本未前进
        self.assertEqual(st.last_cursor_version, 6)

        # 恢复：接续原运行位置完成全量+增量，一次性发布
        recovered = sync_engine.synchronize(db, st)
        self.assertEqual(recovered.id, interrupted.id)
        self.assertEqual(recovered.status, 'published')
        self.assertEqual(recovered.mode, 'full_then_incremental')
        ds = db.get(Dataset, recovered.published_dataset_id)
        keys = sorted(r['operator'] for r in ds.records)
        self.assertNotIn('OP-002', keys)       # 全量期间撤回已纳入
        self.assertIn('OP-007', keys)          # 全量期间新增已纳入
        self.assertEqual(len(keys), 6)
        self.assertEqual(st.last_cursor_version, recovered.source_head_version)
        # 发布后暂存可保留作审计，但不会再被分析侧读取
        self.assertEqual(st.active_run_id, None)
        self.assertIsNone(st.lock_owner)

    # 5. 全量副本之后、追平之前再追加增量，验证接续窗口完整
    def test_05_incremental_during_full_is_stitched(self):
        db = self.db
        st = make_station(db)
        for i in range(1, 4):
            source.append_event(db, st, 'upsert', f'OP-{i:03}', row(f'OP-{i:03}'))
        db.commit()
        sync_engine.synchronize(db, st)
        source.advance_cursor(db, st, 4)  # 严格大于游标 v3 → 过期；日志尚未清理
        db.commit()

        # 先在全量复制开始前放入快照后增量（事件日志保留完整，模拟正常复制窗口）
        source.append_event(db, st, 'upsert', 'OP-004', row('OP-004'))
        source.append_event(db, st, 'upsert', 'OP-005', row('OP-005'))
        db.commit()
        run = sync_engine.synchronize(db, st)
        self.assertEqual(run.status, 'published')
        self.assertEqual(run.mode, 'full_then_incremental')
        ds = db.get(Dataset, run.published_dataset_id)
        self.assertEqual(len(ds.records), 5)
        # 全量开始时 5 个版本均已产生，快照即覆盖全部；drain 无新增
        self.assertEqual(run.snapshot_version, 5)
        self.assertEqual(run.applied_events, 0)
        self.assertEqual(run.applied_version, 5)

    def test_05b_incremental_arrives_after_snapshot(self):
        """快照建立之后才到达的事件，必须由接续增量补齐。"""
        db = self.db
        st = make_station(db)
        for i in range(1, 4):
            source.append_event(db, st, 'upsert', f'OP-{i:03}', row(f'OP-{i:03}'))
        db.commit()
        sync_engine.synchronize(db, st)
        source.advance_cursor(db, st, 4)  # 过期
        db.commit()
        # 注入故障使全量在中途停下，期间再产生 v4/v5
        sync_engine.inject_fault('full_stage')
        with self.assertRaises(RuntimeError):
            sync_engine.synchronize(db, st)
        db.rollback()
        snap = db.scalar(select(SyncRun).where(SyncRun.status == 'interrupted')).snapshot_version
        source.append_event(db, st, 'upsert', 'OP-004', row('OP-004'))
        source.append_event(db, st, 'upsert', 'OP-005', row('OP-005'))
        db.commit()
        run = sync_engine.synchronize(db, st)
        self.assertEqual(run.status, 'published')
        self.assertEqual(run.snapshot_version, snap)
        ds = db.get(Dataset, run.published_dataset_id)
        self.assertEqual(len(ds.records), 5)
        self.assertEqual(run.applied_version, 5)
        # v4/v5 经“转正对账 + drain 去重”纳入，两条路径处理总数必须等于期间变更数
        self.assertEqual(run.applied_events + run.skipped_events, 2)

    # 6. 无法证明连续：保留旧批次，运行标记 gap 并说明缺口范围
    def test_06_gap_keeps_old_batch(self):
        db = self.db
        st = make_station(db)
        for i in range(1, 4):
            source.append_event(db, st, 'upsert', f'OP-{i:03}', row(f'OP-{i:03}'))
        db.commit()
        first = sync_engine.synchronize(db, st)
        datasets_after_first = count(db, Dataset)
        cursor_kept = st.last_cursor_version

        # 全量期间源端持续变更，随后事件日志在恢复前被截断：镜像已更新到 v6，
        # 但保留窗口内应当连续的 v5 事件缺失 → 无法证明期间变更连续
        source.advance_cursor(db, st, 4)  # 触发过期
        source.append_event(db, st, 'upsert', 'OP-004', row('OP-004'))   # v4
        source.append_event(db, st, 'withdraw', 'OP-001', {})           # v5
        source.append_event(db, st, 'upsert', 'OP-005', row('OP-005'))   # v6
        db.commit()
        # 保留窗口推过镜像头 v6：镜像记录还在（全量可见），但快照后事件全部不可读，
        # 无法逐条证明全量期间的变更连续
        source.advance_cursor(db, st, 7)
        db.commit()
        self.assertTrue(source.is_cursor_expired(st))

        run = sync_engine.synchronize(db, st)
        self.assertEqual(run.status, 'gap')
        self.assertIsNone(run.published_dataset_id)
        self.assertIn('无法证明', run.gap_reason)
        self.assertEqual(run.gap_from_version, 7)  # 窗口起点即缺证位置
        # 旧批次原样保留，游标不前进，已有分析的绑定不受影响
        self.assertEqual(count(db, Dataset), datasets_after_first)
        self.assertEqual(st.last_cursor_version, cursor_kept)
        old = db.get(Dataset, first.published_dataset_id)
        self.assertEqual(len(old.records), 3)

        # 缺口说明可在运行记录中复查，且后续重新具备完整全量后可恢复发布
        self.assertTrue(run.note or run.gap_reason)

    # 7. 纯缺口无法恢复时保留旧批次（全量源也被清空的极端情况由 gap 分支保证）
    def test_07_gap_branch_directly(self):
        db = self.db
        st = make_station(db)
        source.append_event(db, st, 'upsert', 'OP-001', row('OP-001'))
        db.commit()
        first = sync_engine.synchronize(db, st)
        run = SyncRun(station_id=st.id, mode='incremental', status='running',
                      created_at=0, updated_at=0)
        db.add(run); db.flush()
        finished = sync_engine._finish_gap(db, st, run, '测试缺口：事件 v2 缺失')
        self.assertEqual(finished.status, 'gap')
        self.assertIsNone(finished.published_dataset_id)
        self.assertEqual(st.last_cursor_version, first.applied_version)
        self.assertEqual(count(db, Dataset), 1)

    # 8. 并发同步同站：锁互斥，不跳过变更，不重复建批次
    def test_08_concurrent_sync_same_station(self):
        db = self.db
        st = make_station(db)
        for i in range(1, 4):
            source.append_event(db, st, 'upsert', f'OP-{i:03}', row(f'OP-{i:03}'))
        db.commit()
        results, errors = [], []

        def worker():
            session = Session()
            try:
                station = session.get(SyncStation, st.id)
                run = sync_engine.synchronize(session, station, owner=f't-{threading.get_ident()}')
                results.append((run.status, run.published_dataset_id))
            except sync_engine.LockBusy as busy:
                errors.append(busy.run_id)
            except Exception as exc:
                errors.append(str(exc))
            finally:
                session.close()

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        published = [r for r in results if r[0] == 'published']
        self.assertEqual(len(published), 1)            # 只发布一个批次
        self.assertEqual(count(db, Dataset), 1)
        # 其余线程要么抢锁失败、要么在发布后重试为 noop，均不得产生第二个批次
        self.assertTrue(all(r[0] in ('published', 'caught_up') for r in results))
        db.refresh(st)
        self.assertEqual(st.last_cursor_version, 3)   # 变更一条没跳过
        self.assertIsNone(st.lock_owner)

    # 9. 已发布批次不可变：后续同步与分析绑定不受影响
    def test_09_published_batch_immutable_and_job_binding(self):
        db = self.db
        st = make_station(db)
        records = seed_records()[:8]
        for r in records:
            source.append_event(db, st, 'upsert', r['operator'], dict(r))
        db.commit()
        run = sync_engine.synchronize(db, st)
        ds = db.get(Dataset, run.published_dataset_id)
        snapshot = [dict(r) for r in ds.records]

        # 绑定一个分析任务到该批次
        job = Job(name='同步批次分析', dataset_id=ds.id, params={'team': '全部', 'confidence': '0.95'},
                  status='queued', created_by=1, created_at=0)
        db.add(job); db.flush()

        # 下一轮同步发布新批次
        source.append_event(db, st, 'upsert', records[0]['operator'],
                            {**records[0], 'before': records[0]['before'] + 50})
        db.commit()
        sync_engine.synchronize(db, st)
        self.assertEqual(db.get(Job, job.id).dataset_id, ds.id)  # 仍绑定原批次
        self.assertEqual([dict(r) for r in db.get(Dataset, ds.id).records], snapshot)

    # 10. 全量副本路径幂等：恢复重试不产生重复暂存/重复批次
    def test_10_full_copy_resume_is_idempotent(self):
        db = self.db
        st = make_station(db)
        for i in range(1, 5):
            source.append_event(db, st, 'upsert', f'OP-{i:03}', row(f'OP-{i:03}'))
        db.commit()
        sync_engine.synchronize(db, st)
        # 保留窗口只越过日志、不越过镜像版本：模拟“全量可见但事件日志被截断”
        source.advance_cursor(db, st, 5)
        db.commit()
        sync_engine.inject_fault('full_stage')
        with self.assertRaises(RuntimeError):
            sync_engine.synchronize(db, st)
        db.rollback()
        run1 = db.scalar(select(SyncRun).where(SyncRun.status == 'interrupted'))
        # 再次故障再恢复，运行仍是同一个
        sync_engine.inject_fault('full_stage')
        with self.assertRaises(RuntimeError):
            sync_engine.synchronize(db, st)
        db.rollback()
        run2 = db.scalar(select(SyncRun).where(SyncRun.status == 'interrupted'))
        self.assertEqual(run1.id, run2.id)
        finished = sync_engine.synchronize(db, st)
        self.assertEqual(finished.id, run1.id)
        self.assertEqual(finished.status, 'published')
        self.assertEqual(count(db, Dataset), 2)  # 首轮 + 恢复后仅一次发布


    # 11. 游标 TTL 超时：即使保留窗口未截断，也走全量副本路径
    def test_11_ttl_timeout_forces_full_copy(self):
        db = self.db
        st = source.create_station(db, 'ST-TTL', 'TTL 演练站', cursor_ttl_seconds=60)
        source.append_event(db, st, 'upsert', 'OP-001', row('OP-001'))
        db.commit()
        first = sync_engine.synchronize(db, st)
        self.assertEqual(first.mode, 'incremental')
        # 回溯上次同步时间，使其超出 TTL
        st.last_synced_at = first.created_at - 120
        db.commit()
        self.assertTrue(source.is_cursor_expired(st))
        source.append_event(db, st, 'upsert', 'OP-002', row('OP-002'))
        db.commit()
        second = sync_engine.synchronize(db, st)
        self.assertEqual(second.mode, 'full_then_incremental')
        self.assertEqual(second.status, 'published')
        self.assertEqual(len(db.get(Dataset, second.published_dataset_id).records), 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
