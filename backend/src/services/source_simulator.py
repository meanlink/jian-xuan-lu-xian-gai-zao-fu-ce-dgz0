"""本地演练用的采集站源端模拟器。

真实环境中这些能力由外部采集站的 API 提供；模拟器用关系表实现等价语义：
- 单调递增的版本号事件日志（upsert / withdraw）
- 源端当前记录镜像（含墓碑），全量复制读这里
- 游标推进（advance_cursor）：截断保留窗口，制造"游标过期"场景
"""
import math
import time

from fastapi import HTTPException
from sqlalchemy import select, delete, func

from src.models import SyncStation, SourceEvent, SourceRecord
from src.services.business import metadata


def _validate_payload(payload: dict):
    """源端即做字段校验，保证进入同步链路的记录可被分析侧消费。"""
    if not isinstance(payload, dict):
        raise HTTPException(422, '源端记录必须是对象')
    for column in metadata()['columns']:
        value = payload.get(column['key'])
        if value is None or value == '':
            raise HTTPException(422, f'源端记录缺少字段 {column["label"]}')
        if column['type'] == 'number':
            try:
                if isinstance(value, bool) or not math.isfinite(float(value)):
                    raise ValueError()
                payload[column['key']] = float(value)
            except (TypeError, ValueError, OverflowError):
                raise HTTPException(422, f'源端字段 {column["label"]} 必须为有限数值')
        else:
            payload[column['key']] = str(value)
    return payload


def create_station(db, code, name, endpoint='local://source-simulator', cursor_ttl_seconds=3600):
    code = code.strip()
    if len(code) < 2:
        raise HTTPException(422, '站点编码至少需要两个字符')
    if db.scalar(select(SyncStation).where(SyncStation.code == code)):
        raise HTTPException(409, '该采集站编码已登记')
    station = SyncStation(code=code, name=name.strip() or code, endpoint=endpoint,
                          cursor_ttl_seconds=cursor_ttl_seconds, created_at=time.time())
    db.add(station)
    db.flush()
    return station


def _next_version(db, station: SyncStation) -> int:
    # 版本号全局单调、永不回收：即使旧事件被保留窗口清理，新版本仍继续递增
    station.version_watermark = int(station.version_watermark or 0) + 1
    return station.version_watermark


def append_event(db, station: SyncStation, op: str, record_key: str, payload: dict | None = None):
    """向源端追加一条变更。同版本号重试不会产生第二条（按自然键幂等）。"""
    if op not in ('upsert', 'withdraw'):
        raise HTTPException(422, '源端事件类型只支持 upsert / withdraw')
    record_key = str(record_key).strip()
    if not record_key:
        raise HTTPException(422, '记录自然键不能为空')
    if op == 'upsert':
        payload = _validate_payload(dict(payload or {}))
    else:
        payload = payload or {}
    version = _next_version(db, station)
    event = SourceEvent(station_id=station.id, version=version, op=op,
                        record_key=record_key, payload=payload, created_at=time.time())
    db.add(event)
    db.flush()
    existing = db.scalar(select(SourceRecord).where(
        SourceRecord.station_id == station.id, SourceRecord.record_key == record_key))
    if existing:
        existing.version = version
        existing.op = op
        existing.payload = payload
        existing.updated_at = time.time()
    else:
        db.add(SourceRecord(station_id=station.id, record_key=record_key, version=version,
                            op=op, payload=payload, updated_at=time.time()))
    db.flush()
    return event


def advance_cursor(db, station: SyncStation, retain_after_version: int):
    """源端推进保留窗口：版本早于边界的事件日志被清理，模拟游标过期/日志截断。

    边界版本及其之后的事件仍可读（retain_after_version 是最早可读版本）。
    """
    retain_after_version = int(retain_after_version)
    if retain_after_version < station.retain_after_version:
        raise HTTPException(422, '保留窗口只能向前推进，不能回退')
    station.retain_after_version = retain_after_version
    db.execute(delete(SourceEvent).where(SourceEvent.station_id == station.id,
                                         SourceEvent.version < retain_after_version))
    db.flush()
    return station


def head_version(db, station_id) -> int:
    # 以站点水位线为准；事件表可能已被保留窗口截断
    station = db.get(SyncStation, station_id)
    return int(station.version_watermark or 0) if station else 0


def pending_events(db, station: SyncStation, after_version: int, limit: int = 500):
    """读取严格晚于游标、且仍在保留窗口内的事件，按版本升序。"""
    return list(db.scalars(select(SourceEvent).where(
        SourceEvent.station_id == station.id,
        SourceEvent.version > after_version,
        SourceEvent.version >= station.retain_after_version
    ).order_by(SourceEvent.version.asc()).limit(limit)))


def live_records(db, station_id):
    """源端当前未撤回记录（全量复制的数据源），按自然键排序保证确定性。"""
    return list(db.scalars(select(SourceRecord).where(
        SourceRecord.station_id == station_id,
        SourceRecord.op == 'upsert').order_by(SourceRecord.record_key.asc())))


def all_records(db, station_id):
    """源端全部镜像（含墓碑），供同步页核对撤回状态。"""
    return list(db.scalars(select(SourceRecord).where(
        SourceRecord.station_id == station_id).order_by(SourceRecord.record_key.asc())))


def is_cursor_expired(station: SyncStation) -> bool:
    """游标过期有两种等价情形：

    1. 本地游标落在源端保留窗口之外（事件日志已截断，无法增量接续）；
    2. 距上次成功同步超过站点配置的游标有效期 TTL（源端不再保证窗口可读）。
    """
    if station.last_cursor_version < station.retain_after_version:
        return True
    if station.last_synced_at and station.cursor_ttl_seconds:
        if time.time() - float(station.last_synced_at) > station.cursor_ttl_seconds:
            return True
    return False
