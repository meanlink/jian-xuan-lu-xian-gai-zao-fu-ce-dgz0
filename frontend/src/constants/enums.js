export const Role={admin:'业务负责人',analyst:'分析员',viewer:'观察员'};
export const JobStatus={queued:'排队中',running:'计算中',completed:'已完成',failed:'执行失败'};
export const SyncRunStatus={running:'同步中',interrupted:'中断待恢复',caught_up:'已追平无变更',published:'已发布批次',gap:'存在缺口未发布',failed:'执行失败'};
export const SyncRunMode={full_then_incremental:'全量副本+增量接续',incremental:'增量同步'};
export const SyncOp={upsert:'新增/更新',withdraw:'撤回'};
