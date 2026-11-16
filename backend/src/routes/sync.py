from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func

from src.config.database import get_db
from src.middlewares.auth import current_user, can_write
from src.models import (SyncStation, SyncRun, SyncRecordState, SyncStaging,
                        SourceEvent, Dataset)
from src.schemas.sync import StationInput, SourceEventInput, CursorAdvanceInput
from src.services import source_simulator as source
from src.services import sync_engine
from src.services.audit import record
from src.utils.serialization import (station_dict, run_dict, source_event_dict,
                                     source_record_dict, record_state_dict)

router = APIRouter(prefix='/sync')


def _get_station(db, station_id: int) -> SyncStation:
    station = db.get(SyncStation, station_id)
    if not station:
        raise HTTPException(404, '采集站不存在')
    return station


def _station_progress(db, station: SyncStation) -> dict:
    head = source.head_version(db, station.id)
    pending = max(0, head - station.last_cursor_version)
    expired = source.is_cursor_expired(station)
    pending_changes = db.scalar(select(func.count()).select_from(SourceEvent).where(
        SourceEvent.station_id == station.id,
        SourceEvent.version > station.last_cursor_version))
    return {
        'source_head_version': head,
        'local_cursor_version': station.last_cursor_version,
        'retain_after_version': station.retain_after_version,
        'cursor_ttl_seconds': station.cursor_ttl_seconds,
        'last_synced_at': station.last_synced_at,
        'pending_event_count': int(pending_changes or 0),
        'pending_version_span': pending,
        'cursor_expired': expired,
        'active_run_id': station.active_run_id,
    }


@router.get('/stations')
def stations(db=Depends(get_db), u=Depends(current_user)):
    result = []
    for station in db.scalars(select(SyncStation).order_by(SyncStation.id.asc())):
        item = station_dict(station)
        item['progress'] = _station_progress(db, station)
        result.append(item)
    return result


@router.post('/stations', status_code=201)
def create_station(data: StationInput, db=Depends(get_db), u=Depends(can_write)):
    station = source.create_station(db, data.code, data.name, data.endpoint,
                                    data.cursor_ttl_seconds)
    record(db, u.email, 'sync.station.create', {'id': station.id, 'code': station.code})
    db.commit()
    return station_dict(station)


@router.get('/stations/{station_id}')
def station_detail(station_id: int, db=Depends(get_db), u=Depends(current_user)):
    station = _get_station(db, station_id)
    runs = [run_dict(r) for r in db.scalars(select(SyncRun).where(
        SyncRun.station_id == station_id).order_by(SyncRun.id.desc()).limit(20))]
    return {
        'station': station_dict(station),
        'progress': _station_progress(db, station),
        'runs': runs,
        'source_records': [source_record_dict(r) for r in source.all_records(db, station_id)],
        'local_states': [record_state_dict(r) for r in db.scalars(select(SyncRecordState).where(
            SyncRecordState.station_id == station_id).order_by(SyncRecordState.record_key.asc()))],
        'staging_runs': [dict(run_id=rid, rows=db.scalar(select(func.count()).select_from(
            SyncStaging).where(SyncStaging.run_id == rid)))
            for rid in db.scalars(select(SyncStaging.run_id).distinct())],
    }


@router.post('/stations/{station_id}/sync')
def trigger_sync(station_id: int, db=Depends(get_db), u=Depends(can_write)):
    station = _get_station(db, station_id)
    try:
        run = sync_engine.synchronize(db, station, owner=u.email)
    except sync_engine.LockBusy as busy:
        raise HTTPException(409, str(busy))
    record(db, u.email, 'sync.run.finish',
           {'id': run.id, 'status': run.status, 'mode': run.mode,
            'dataset_id': run.published_dataset_id})
    db.commit()
    return run_dict(db.get(SyncRun, run.id))


@router.get('/stations/{station_id}/runs')
def runs(station_id: int, db=Depends(get_db), u=Depends(current_user)):
    _get_station(db, station_id)
    return [run_dict(r) for r in db.scalars(select(SyncRun).where(
        SyncRun.station_id == station_id).order_by(SyncRun.id.desc()).limit(50))]


@router.get('/stations/{station_id}/pending')
def pending(station_id: int, db=Depends(get_db), u=Depends(current_user)):
    station = _get_station(db, station_id)
    events = source.pending_events(db, station, station.last_cursor_version, limit=1000)
    return {
        'progress': _station_progress(db, station),
        'events': [source_event_dict(e) for e in events],
    }


# ---- 以下为本地演练的源端模拟器接口；真实部署替换为采集站官方 API ----

@router.post('/stations/{station_id}/source/events', status_code=201)
def push_source_event(station_id: int, data: SourceEventInput,
                      db=Depends(get_db), u=Depends(can_write)):
    station = _get_station(db, station_id)
    event = source.append_event(db, station, data.op, data.record_key, data.payload)
    record(db, u.email, 'sync.source.event',
           {'station': station.code, 'version': event.version,
            'op': data.op, 'record_key': data.record_key})
    db.commit()
    return source_event_dict(event)


@router.post('/stations/{station_id}/source/advance-cursor')
def advance_cursor(station_id: int, data: CursorAdvanceInput,
                   db=Depends(get_db), u=Depends(can_write)):
    station = _get_station(db, station_id)
    source.advance_cursor(db, station, data.retain_after_version)
    record(db, u.email, 'sync.source.advance',
           {'station': station.code, 'retain_after_version': data.retain_after_version})
    db.commit()
    return station_dict(station)


@router.get('/stations/{station_id}/source/events')
def list_source_events(station_id: int, db=Depends(get_db), u=Depends(current_user)):
    station = _get_station(db, station_id)
    events = db.scalars(select(SourceEvent).where(
        SourceEvent.station_id == station_id).order_by(SourceEvent.version.desc()).limit(100))
    return [source_event_dict(e) for e in events]
