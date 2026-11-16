from sqlalchemy import String, JSON, Double, Integer, Boolean, ForeignKey, Text
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class SyncStation(Base):
    """采集站登记：每个外部站点一行，游标按站点独立保存。"""
    __tablename__ = 'sync_stations'
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(80), unique=True)
    name: Mapped[str] = mapped_column(String(160))
    endpoint: Mapped[str] = mapped_column(String(240), default='local://source-simulator')
    # 游标有效期（秒）；源端最早事件版本超过 retain_after_version 即判定游标过期
    cursor_ttl_seconds: Mapped[int] = mapped_column(Integer, default=3600)
    retain_after_version: Mapped[int] = mapped_column(Integer, default=0)
    # 源端单调版本水位线：事件日志可被保留窗口清理，版本号本身永不回收
    version_watermark: Mapped[int] = mapped_column(Integer, default=0)
    last_cursor_version: Mapped[int] = mapped_column(Integer, default=0)
    last_synced_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    active_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lock_owner: Mapped[str | None] = mapped_column(String(80), nullable=True)
    lock_until: Mapped[float | None] = mapped_column(Double, nullable=True)
    created_at: Mapped[float] = mapped_column(Double)


class SourceEvent(Base):
    """模拟采集站的源端变更日志（本地演练用，真实环境由采集站提供等价接口）。"""
    __tablename__ = 'source_events'
    id: Mapped[int] = mapped_column(primary_key=True)
    station_id: Mapped[int] = mapped_column(ForeignKey('sync_stations.id'), index=True)
    version: Mapped[int] = mapped_column(Integer, index=True)
    op: Mapped[str] = mapped_column(String(20))  # upsert / withdraw
    record_key: Mapped[str] = mapped_column(String(160))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[float] = mapped_column(Double)


class SourceRecord(Base):
    """源端当前记录镜像（含墓碑），供全量复制读取。"""
    __tablename__ = 'source_records'
    id: Mapped[int] = mapped_column(primary_key=True)
    station_id: Mapped[int] = mapped_column(ForeignKey('sync_stations.id'), index=True)
    record_key: Mapped[str] = mapped_column(String(160))
    version: Mapped[int] = mapped_column(Integer, default=0)
    op: Mapped[str] = mapped_column(String(20), default='upsert')
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[float] = mapped_column(Double)


class SyncRecordState(Base):
    """本地已确认的逐记录版本与墓碑：乱序/重复不倒退，撤回不被旧消息复活。"""
    __tablename__ = 'sync_record_states'
    id: Mapped[int] = mapped_column(primary_key=True)
    station_id: Mapped[int] = mapped_column(ForeignKey('sync_stations.id'), index=True)
    record_key: Mapped[str] = mapped_column(String(160))
    version: Mapped[int] = mapped_column(Integer, default=0)
    op: Mapped[str] = mapped_column(String(20), default='upsert')
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[float] = mapped_column(Double)


class SyncRun(Base):
    """一次同步运行：全量副本与增量追平均登记在此，半成品只通过 staging 可见。"""
    __tablename__ = 'sync_runs'
    id: Mapped[int] = mapped_column(primary_key=True)
    station_id: Mapped[int] = mapped_column(ForeignKey('sync_stations.id'), index=True)
    mode: Mapped[str] = mapped_column(String(20))  # full_then_incremental / incremental
    status: Mapped[str] = mapped_column(String(24), default='running')
    # running / interrupted / caught_up / published / gap / failed
    start_cursor_version: Mapped[int] = mapped_column(Integer, default=0)
    snapshot_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    applied_version: Mapped[int] = mapped_column(Integer, default=0)
    source_head_version: Mapped[int] = mapped_column(Integer, default=0)
    applied_events: Mapped[int] = mapped_column(Integer, default=0)
    skipped_events: Mapped[int] = mapped_column(Integer, default=0)
    gap_from_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    gap_reason: Mapped[str] = mapped_column(String(300), default='')
    published_dataset_id: Mapped[int | None] = mapped_column(ForeignKey('datasets.id'), nullable=True)
    publish_basis: Mapped[dict] = mapped_column(JSON, default=dict)
    owner: Mapped[str] = mapped_column(String(80), default='')
    note: Mapped[str] = mapped_column(Text, default='')
    created_at: Mapped[float] = mapped_column(Double)
    updated_at: Mapped[float] = mapped_column(Double)


class SyncStaging(Base):
    """全量副本暂存区：按 run 隔离，未发布前对分析侧完全不可见。"""
    __tablename__ = 'sync_staging'
    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey('sync_runs.id'), index=True)
    station_id: Mapped[int] = mapped_column(ForeignKey('sync_stations.id'), index=True)
    record_key: Mapped[str] = mapped_column(String(160))
    version: Mapped[int] = mapped_column(Integer)
    op: Mapped[str] = mapped_column(String(20))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
