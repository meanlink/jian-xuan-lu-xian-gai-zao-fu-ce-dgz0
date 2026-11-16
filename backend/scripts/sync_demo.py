"""采集站同步的引导式本地演练（无需 MySQL/Redis，默认使用临时 SQLite）。

运行：
    cd backend && python -m scripts.sync_demo
剧情：
  1. 增量同步发布不可变批次，同进度重试不新增批次；
  2. 撤回写墓碑，乱序低版本不能复活；
  3. 推进源端保留窗口使游标过期，全量副本期间继续产生新增/撤回，
     注入故障模拟中断，恢复后一次性发布，期间变更全部纳入；
  4. 第二个站点制造无法证明连续的缺口，旧批次保留并说明缺口。
"""
import os
import tempfile

_DB = os.path.join(tempfile.gettempdir(), 'sync_demo_walkthrough.db')
if os.path.exists(_DB):
    os.remove(_DB)
os.environ.setdefault('DATABASE_URL', f'sqlite:///{_DB}')

from sqlalchemy import select

from src.config.database import Base, engine, Session
from src.models import Dataset, SyncRun
from src.services import source_simulator as source
from src.services import sync_engine

Base.metadata.create_all(engine)


def row(key, before=402.0, after=368.0):
    return {'operator': key, 'team': '一组', 'before': before, 'after': after}


def line(title):
    print('\n' + '=' * 72 + f'\n {title}\n' + '=' * 72)


def show_run(tag, run):
    print(f'[{tag}] 运行 #{run.id} 模式={run.mode} 状态={run.status} '
          f'游标 v{run.start_cursor_version}→v{run.applied_version} '
          f'应用={run.applied_events} 跳过={run.skipped_events} '
          f'批次={run.published_dataset_id or "—"}')
    if run.gap_reason:
        print(f'        缺口：{run.gap_reason}')


def main():
    with Session() as db:
        line('1. 登记采集站并投递 5 条变更（含 1 条撤回）')
        st = source.create_station(db, 'ST-WALK', '走查演练采集站', cursor_ttl_seconds=3600)
        for i in range(1, 5):
            source.append_event(db, st, 'upsert', f'OP-{i:03}', row(f'OP-{i:03}'))
        source.append_event(db, st, 'withdraw', 'OP-001', {})
        db.commit()
        run = sync_engine.synchronize(db, st, owner='demo')
        show_run('首次增量', run)
        ds = db.get(Dataset, run.published_dataset_id)
        print(f'        发布批次《{ds.name}》有效记录 {len(ds.records)} 条，'
              f'指纹 {ds.checksum[:16]}…')

        line('2. 相同进度重试（应 noop，不新增批次）')
        again = sync_engine.synchronize(db, st, owner='demo')
        show_run('重试', again)
        total_datasets = len(db.scalars(select(Dataset)).all())
        print(f'        当前批次总数：{total_datasets}（重试未新增）')

        line('3. 乱序/重复不倒退：手工投递一条更低版本事件，必须被跳过')
        from src.models import SourceEvent
        probe = SyncRun(station_id=st.id, mode='incremental', status='running',
                        created_at=0, updated_at=0)
        db.add(probe); db.flush()
        stale = SourceEvent(station_id=st.id, version=1, op='upsert',
                            record_key='OP-002', payload=row('OP-002', 1, 1), created_at=0)
        applied = sync_engine._apply_event(db, st.id, stale, probe)
        probe.status = 'caught_up'
        probe.note = '乱序探针：低版本事件被版本单调规则拒绝'
        db.commit()
        print(f'        低版本事件是否生效：{applied}（应为 False），累计跳过 {probe.skipped_events} 条')

        line('4. 游标过期：推进保留窗口，全量副本期间再产生新增与撤回')
        source.advance_cursor(db, st, st.version_watermark + 1)  # 窗口越过本地游标
        db.commit()
        print(f'        游标过期判定：{source.is_cursor_expired(st)}')
        source.append_event(db, st, 'upsert', 'OP-009', row('OP-009'))
        source.append_event(db, st, 'withdraw', 'OP-002', {})
        db.commit()
        sync_engine.inject_fault('full_stage')
        try:
            sync_engine.synchronize(db, st, owner='demo')
        except RuntimeError as exc:
            print(f'        注入故障模拟进程中断：{exc}')
        db.rollback()
        interrupted = db.scalar(select(SyncRun).where(SyncRun.status == 'interrupted'))
        print(f'        半成品只在暂存区，未发布批次；中断运行 #{interrupted.id} 等待恢复')

        line('5. 恢复原同步位置：复用同一运行，追平后一次性发布')
        recovered = sync_engine.synchronize(db, st, owner='demo')
        show_run('恢复', recovered)
        new_ds = db.get(Dataset, recovered.published_dataset_id)
        keys = sorted(r['operator'] for r in new_ds.records)
        print(f'        新批次有效记录 {len(keys)} 条：{keys}')
        print(f'        期间撤回 OP-002 已纳入：{"OP-002" not in keys}；'
              f'期间新增 OP-009 已纳入：{"OP-009" in keys}')
        print(f'        发布依据：{recovered.publish_basis}')

        line('6. 无法证明连续：第二个站点日志被彻底截断，保留旧批次')
        st2 = source.create_station(db, 'ST-GAP', '缺口演练采集站')
        source.append_event(db, st2, 'upsert', 'OP-101', row('OP-101'))
        db.commit()
        first2 = sync_engine.synchronize(db, st2, owner='demo')
        show_run('缺口站首次', first2)
        # 全量期间产生变更，随后窗口越过镜像头，期间事件全部不可读
        source.advance_cursor(db, st2, st2.version_watermark + 1)
        source.append_event(db, st2, 'upsert', 'OP-102', row('OP-102'))
        source.append_event(db, st2, 'withdraw', 'OP-101', {})
        db.commit()
        source.advance_cursor(db, st2, st2.version_watermark + 1)
        db.commit()
        gap_run = sync_engine.synchronize(db, st2, owner='demo')
        show_run('缺口站同步', gap_run)
        print(f'        旧批次仍在：{db.get(Dataset, first2.published_dataset_id).name}')

        line('结果汇总')
        for r in db.scalars(select(SyncRun).order_by(SyncRun.id)):
            show_run('汇总', r)
        print(f'\n演练库：{_DB}（删除该文件即可重来）')


if __name__ == '__main__':
    main()
