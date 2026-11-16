from enum import Enum
class Role(str, Enum):
    admin = 'admin'
    analyst = 'analyst'
    viewer = 'viewer'
class JobStatus(str, Enum):
    queued = 'queued'
    running = 'running'
    completed = 'completed'
    failed = 'failed'
class SyncRunMode(str, Enum):
    full_then_incremental = 'full_then_incremental'
    incremental = 'incremental'
class SyncRunStatus(str, Enum):
    running = 'running'
    interrupted = 'interrupted'
    caught_up = 'caught_up'
    published = 'published'
    gap = 'gap'
    failed = 'failed'
SYNC_RUN_STATUS_LABELS = {
    'running': '同步中',
    'interrupted': '中断待恢复',
    'caught_up': '已追平无变更',
    'published': '已发布批次',
    'gap': '存在缺口未发布',
    'failed': '执行失败',
}
SYNC_OP_LABELS = {'upsert': '新增/更新', 'withdraw': '撤回'}
