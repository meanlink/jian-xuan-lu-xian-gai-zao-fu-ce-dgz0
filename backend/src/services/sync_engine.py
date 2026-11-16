"""采集站同步引擎。

关键不变量：
1. 逐记录版本单调：只有 event.version > 本地版本才应用，乱序/重复不倒退。
2. 撤回写墓碑：withdraw 墓碑同样带版本，低版本 upsert 无法复活已撤回记录。
3. 游标过期先建全量副本（staging 对分析侧不可见），再接续增量，追平后一次性
   原子发布为新的不可变 Dataset；半成品任何时刻都不可见。
4. 中断恢复原同步位置：运行按 run 恢复，已确认的游标与逐记录版本不回退。
5. 相同进度重试是无操作，只登记 noop 运行，不新增批次。
6. 站点行锁保证并发同站只有一个执行者，其他调用等待/复用，不跳过变更。
7. 无法证明连续（缺口）时保留旧批次，运行标记 gap 并说明缺口范围，不发布。
"""
import os
import time
import uuid
import logging

from sqlalchemy import select, func, update

from src.models import (SyncStation, SourceEvent, SourceRecord, SyncRecordState,
                        SyncRun, SyncStaging, Dataset)
from src.services import source_simulator as source
from src.utils.serialization import checksum

logger = logging.getLogger(__name__)

# 测试/演练用故障注入钩子：例如 {'full_stage': raise_fn}，触发后自动清除
_FAULTS: dict = {}
STAGE_CHUNK = int(os.getenv('SYNC_STAGE_CHUNK', '200'))


def inject_fault(stage: str):
    _FAULTS[stage] = True


def _maybe_fault(stage: str):
    if _FAULTS.pop(stage, False):
        raise RuntimeError(f'fault injected at stage {stage}')


def _materialize_records(db, station_id: int) -> list[dict]:
    """从本地已确认状态物化当前可见记录（墓碑除外），按自然键排序。"""
    rows = []
    for state in db.scalars(select(SyncRecordState).where(
            SyncRecordState.station_id == station_id,
            SyncRecordState.op == 'upsert').order_by(SyncRecordState.record_key.asc())):
        rows.append(dict(state.payload))
    return rows


def _publish_dataset(db, station: SyncStation, run: SyncRun, basis_extra: dict | None = None):
    """把已确认状态一次性发布为不可变分析批次，返回新 Dataset。"""
    records = _materialize_records(db, station.id)
    base = f'{station.name} · 采集站同步'
    name = f'{base} · 截至版本 {run.applied_version}'
    # 同名冲突时追加运行编号，保证批次名唯一且可追溯
    if db.scalar(select(Dataset).where(Dataset.name == name)):
        name = f'{base} · 运行 {run.id} · 截至版本 {run.applied_version}'
    dataset = Dataset(
        name=name,
        source=f'sync://{station.code}#v{run.applied_version}',
        records=records,
        checksum=checksum(records),
        created_at=time.time())
    db.add(dataset)
    db.flush()
    run.published_dataset_id = dataset.id
    run.publish_basis = {
        'station_code': station.code,
        'run_id': run.id,
        'mode': run.mode,
        'start_cursor_version': run.start_cursor_version,
        'snapshot_version': run.snapshot_version,
        'applied_version': run.applied_version,
        'source_head_version': run.source_head_version,
        'applied_events': run.applied_events,
        'skipped_events': run.skipped_events,
        'record_count': len(records),
        'checksum': dataset.checksum,
        'published_at': dataset.created_at,
        **(basis_extra or {}),
    }
    return dataset


class LockBusy(Exception):
    def __init__(self, run_id):
        self.run_id = run_id
        super().__init__(f'同一采集站正在同步（运行 {run_id}），已复用该运行的最新结果')


def _acquire_lock(db, station: SyncStation, owner: str, ttl: float = 300.0):
    """站点级互斥锁。SQLite/MySQL 均用条件 UPDATE 原子抢锁，避免方言差异。"""
    now = time.time()
    refreshed = db.execute(update(SyncStation).where(
        SyncStation.id == station.id,
        ((SyncStation.lock_owner == owner) |
         (SyncStation.lock_until.is_(None)) |
         (SyncStation.lock_until < now))
    ).values(lock_owner=owner, lock_until=now + ttl)).rowcount
    db.flush()
    if not refreshed:
        active = db.get(SyncStation, station.id)
        raise LockBusy(active.active_run_id)
    db.refresh(station)


def _release_lock(db, station: SyncStation, owner: str):
    db.execute(update(SyncStation).where(
        SyncStation.id == station.id, SyncStation.lock_owner == owner
    ).values(lock_owner=None, lock_until=None, active_run_id=None))
    db.flush()


def _recover_interrupted(db, station: SyncStation):
    """上次崩溃留下的运行统一标记为中断待恢复，锁凭 TTL 可被重新接管。"""
    if station.active_run_id:
        stale = db.get(SyncRun, station.active_run_id)
        if stale and stale.status == 'running':
            stale.status = 'interrupted'
            stale.note = (stale.note or '') + '\n进程中断，恢复后从原游标位置接续。'
            stale.updated_at = time.time()
            db.flush()
    station.active_run_id = None
    db.flush()


def _apply_event(db, station_id: int, event: SourceEvent, run: SyncRun):
    """按版本单调应用单条事件；重复或乱序（版本不更高）只计数不落库。"""
    state = db.scalar(select(SyncRecordState).where(
        SyncRecordState.station_id == station_id,
        SyncRecordState.record_key == event.record_key))
    if state is not None and event.version <= state.version:
        run.skipped_events += 1
        return False
    if state is None:
        state = SyncRecordState(station_id=station_id, record_key=event.record_key,
                                version=event.version, op=event.op, payload=event.payload,
                                updated_at=time.time())
        db.add(state)
    else:
        state.version = event.version
        state.op = event.op
        state.payload = event.payload
        state.updated_at = time.time()
    run.applied_events += 1
    return True


def _drain_events(db, station: SyncStation, run: SyncRun, after_version: int) -> int:
    """顺序消费增量事件直到源端当前头部；返回新的游标版本。

    事件版本是全局单调日志序列，必须严格连续：期望的下一个版本缺失即证明
    存在缺口，拒绝猜测式接续。
    """
    cursor = after_version
    empty_polls = 0
    while empty_polls < 2:
        head = source.head_version(db, station.id)
        events = source.pending_events(db, station, cursor, limit=STAGE_CHUNK)
        if not events:
            run.source_head_version = head
            if cursor >= head:
                break
            # 游标落后但读不到事件：保留窗口已截断，连续性无法证明
            run.gap_from_version = max(cursor + 1, station.retain_after_version)
            run.gap_reason = (f'需要版本 {run.gap_from_version} 起的事件，但源端保留窗口已从 '
                              f'v{station.retain_after_version} 开始，事件缺失无法接续。')
            raise _GapError(run.gap_reason)
        # 窗口外的历史版本不可得（由全量副本重建）；窗口内必须严格连续
        first = events[0].version
        if first < station.retain_after_version:
            raise _GapError('内部错误：保留窗口外事件不应被读取')
        expected_next = max(cursor + 1, station.retain_after_version)
        if first != expected_next:
            run.gap_from_version = expected_next
            run.gap_reason = (f'事件序列不连续：本地游标 v{cursor} 之后最早只见到 '
                              f'v{first}，缺少 v{expected_next}。')
            raise _GapError(run.gap_reason)
        for event in events:
            if event.version != expected_next:
                run.gap_from_version = expected_next
                run.gap_reason = (f'事件序列在 v{cursor} 之后断裂，缺少 v{expected_next}，'
                                  f'无法证明变更连续。')
                raise _GapError(run.gap_reason)
            _apply_event(db, station.id, event, run)
            cursor = event.version
            expected_next = cursor + 1
        run.applied_version = cursor
        run.updated_at = time.time()
        db.commit()  # 分段提交：中断后已确认游标不丢失，恢复不重做
        empty_polls = empty_polls + 1 if cursor >= head else 0
    run.source_head_version = source.head_version(db, station.id)
    return cursor


def _mirror_max_version(db, station_id) -> int:
    return int(db.scalar(select(func.coalesce(func.max(SourceRecord.version), 0))
                         .where(SourceRecord.station_id == station_id)))


class _GapError(Exception):
    pass


def _run_full_then_incremental(db, station: SyncStation, run: SyncRun):
    """游标过期路径：全量副本 → 接续增量 → 追平后原子切换。"""
    # 1) 全量副本写入按 run 隔离的暂存区，半成品对分析侧不可见。
    #    恢复重跑时保留首次全量的快照点，保证增量接续位置不漂移。
    db.execute(SyncStaging.__table__.delete().where(SyncStaging.run_id == run.id))
    db.flush()
    records = source.live_records(db, station.id)
    if run.snapshot_version is None:
        run.snapshot_version = source.head_version(db, station.id)
    snapshot_version = run.snapshot_version
    staged = 0
    for record in records:
        db.add(SyncStaging(run_id=run.id, station_id=station.id,
                           record_key=record.record_key, version=record.version,
                           op=record.op, payload=record.payload))
        staged += 1
        if staged % STAGE_CHUNK == 0:
            run.updated_at = time.time()
            db.commit()
            _maybe_fault('full_stage')
    db.commit()
    _maybe_fault('full_stage')

    # 2) 暂存确认完整后，一次性转正为本地逐记录版本基线（仍不发布批次）
    db.execute(SyncRecordState.__table__.delete().where(SyncRecordState.station_id == station.id))
    for item in db.scalars(select(SyncStaging).where(SyncStaging.run_id == run.id)):
        db.add(SyncRecordState(station_id=station.id, record_key=item.record_key,
                               version=item.version, op=item.op, payload=item.payload,
                               updated_at=time.time()))
    # 全量期间发生的撤回也要纳入：源端墓碑覆盖暂存中的旧版本
    for mirror in source.all_records(db, station.id):
        state = db.scalar(select(SyncRecordState).where(
            SyncRecordState.station_id == station.id,
            SyncRecordState.record_key == mirror.record_key))
        if state is None or mirror.version > state.version:
            if state is None:
                db.add(SyncRecordState(station_id=station.id, record_key=mirror.record_key,
                                       version=mirror.version, op=mirror.op,
                                       payload=mirror.payload, updated_at=time.time()))
            else:
                state.version, state.op, state.payload = mirror.version, mirror.op, mirror.payload
    # 对账后已确认的最高版本可能高于快照点（全量期间的变更已随镜像纳入）
    run.applied_version = max(snapshot_version, _mirror_max_version(db, station.id))
    db.commit()
    _maybe_fault('promote_stage')

    # 3) 接续前的连续性兜底：全量期间的事实更新都晚于本地旧游标。对
    #    (旧游标, 镜像头] 区间内落在保留窗口的版本，事件日志必须齐全；
    #    窗口外版本不可得由全量副本重建，不作为缺口。窗口内缺证、或整个
    #    窗口都越过镜像头，即无法证明全量期间变更连续，保留原批次不发布。
    mirror_head = _mirror_max_version(db, station.id)
    baseline = run.start_cursor_version
    if mirror_head > baseline:
        floor = max(baseline + 1, station.retain_after_version)
        if floor > mirror_head:
            run.gap_from_version = floor
            run.gap_reason = (f'全量复制期间产生了 v{baseline + 1}–v{mirror_head} '
                              f'的变更，但源端保留窗口已从 v{station.retain_after_version} '
                              f'开始，期间事件全部不可读，无法证明变更连续，保留原批次。')
            return _finish_gap(db, station, run, run.gap_reason)
        available = set(db.scalars(select(SourceEvent.version).where(
            SourceEvent.station_id == station.id,
            SourceEvent.version >= floor,
            SourceEvent.version <= mirror_head)).all())
        expected = set(range(floor, mirror_head + 1))
        missing = sorted(expected - available)
        if missing:
            run.gap_from_version = missing[0]
            run.gap_reason = (f'全量复制期间的事件日志缺失版本 {missing[:5]}'
                              f'{"…" if len(missing) > 5 else ""}，无法证明期间变更连续，'
                              f'保留原批次不发布。')
            return _finish_gap(db, station, run, run.gap_reason)

    # 4) 接续全量复制期间（含复制进行中）的增量，追平顺带覆盖新增与撤回
    try:
        cursor = _drain_events(db, station, run, snapshot_version)
    except _GapError as exc:
        return _finish_gap(db, station, run, str(exc))

    # 5) 追平后一次性切换为可分析批次
    dataset = _publish_dataset(db, station, run, {'full_copy_records': len(records)})
    station.last_cursor_version = cursor
    station.last_synced_at = time.time()
    run.status = 'published'
    run.updated_at = time.time()
    db.commit()
    return run


def _run_incremental(db, station: SyncStation, run: SyncRun):
    """游标有效路径：仅消费增量；无变更为 noop，不新增批次。"""
    head = source.head_version(db, station.id)
    run.source_head_version = head
    if station.last_cursor_version >= head:
        run.applied_version = station.last_cursor_version
        run.status = 'caught_up'
        run.note = '源端无新版本，相同进度重试不新增批次。'
        run.updated_at = time.time()
        db.commit()
        return run
    try:
        cursor = _drain_events(db, station, run, station.last_cursor_version)
    except _GapError as exc:
        # 增量途中发现日志被截断：降级为全量副本路径，旧批次继续保留
        run.mode = 'full_then_incremental'
        run.note = (run.note or '') + f'\n增量接续发现缺口，转全量副本：{exc}'
        db.commit()
        return _run_full_then_incremental(db, station, run)

    dataset = _publish_dataset(db, station, run)
    station.last_cursor_version = cursor
    station.last_synced_at = time.time()
    run.status = 'published'
    run.updated_at = time.time()
    db.commit()
    return run


def _finish_gap(db, station: SyncStation, run: SyncRun, reason: str):
    run.status = 'gap'
    run.gap_reason = reason or run.gap_reason or '连续性无法证明'
    run.updated_at = time.time()
    # 游标不前进、不发布；旧批次保持不动，已有分析继续绑定原批次
    db.commit()
    return run


def synchronize(db, station: SyncStation, owner: str | None = None) -> SyncRun:
    """执行一次同步。并发同站时复用进行中的运行，不重复建批次。"""
    owner = owner or f'api-{uuid.uuid4().hex[:10]}'
    _recover_interrupted(db, station)
    try:
        _acquire_lock(db, station, owner)
    except LockBusy as busy:
        db.rollback()
        existing = db.get(SyncRun, busy.run_id) if busy.run_id else None
        if existing and existing.status in ('published', 'caught_up', 'gap'):
            return existing
        raise

    expired = source.is_cursor_expired(station)
    # 中断的全量运行直接复用原 run，保留已暂存进度
    resume = db.scalar(select(SyncRun).where(
        SyncRun.station_id == station.id,
        SyncRun.status == 'interrupted').order_by(SyncRun.id.desc()))
    if resume:
        run = resume
        run.status = 'running'
        run.owner = owner
        run.updated_at = time.time()
    else:
        run = SyncRun(station_id=station.id,
                      mode='full_then_incremental' if expired else 'incremental',
                      status='running', start_cursor_version=station.last_cursor_version,
                      applied_version=station.last_cursor_version,
                      source_head_version=source.head_version(db, station.id),
                      owner=owner, created_at=time.time(), updated_at=time.time())
        db.add(run)
    db.flush()
    station.active_run_id = run.id
    db.commit()
    try:
        if run.mode == 'full_then_incremental':
            finished = _run_full_then_incremental(db, station, run)
        else:
            finished = _run_incremental(db, station, run)
    except Exception:
        db.rollback()
        failed = db.get(SyncRun, run.id)
        if failed and failed.status == 'running':
            failed.status = 'interrupted'
            failed.note = (failed.note or '') + '\n执行异常中断，可重试恢复。'
            failed.updated_at = time.time()
            db.commit()
        logger.exception('station %s sync run %s failed', station.code, run.id)
        raise
    finally:
        _FAULTS.clear()  # 故障钩子只对当次注入有效，恢复重试不再受残留影响
        try:
            _release_lock(db, station, owner)
            db.commit()
        except Exception:
            db.rollback()
    return finished
