# 拣选路线改造复测

配对实验分析的真实业务分析小型项目。完整模拟客户、痛点、上下游、算法公式与一天工作剧本见 [BUSINESS.md](BUSINESS.md)。

## 模块与业务流程
数据接入（JSON/CSV、字段验证、批次指纹）→ 参数化分析（异步 Python worker）→ 结果复核（指标、图形、明细、解释、批准）→ 定时分发（本地收件箱、CSV/JSON、SHA256）；另有业务总览和审计轨迹。
三个贯穿全栈的实体：Dataset 数据批次、Job 分析任务、Report 分发计划；辅助 User、Audit、Delivery。

## 快速启动
```bash
cp .env.example .env
docker compose up -d --build --wait
docker compose exec -T backend python -m scripts.seed_demo
```
访问 http://localhost:18205；后端文档 http://localhost:19205/docs；数据库宿主端口 17705。
`SEED_DEMO=true` 自动幂等 seed；不覆盖用户修改。手动 seed 命令可重复执行。

## 演示账号与权限
|账号|密码|真实权限|
|---|---|---|
|admin@demo.local|Demo123!|接入、执行、批准、分发、暂停、读取与下载|
|analyst@demo.local|Demo123!|接入、执行、读取与下载；不能批准或安排分发|
|viewer@demo.local|Demo123!|只读与下载|

## 技术与本地开发
Python 3.12 / FastAPI / SQLAlchemy / NumPy / SciPy / scikit-learn；MySQL 8.0；Redis 7；Nginx；原生 HTML CSS JavaScript ES modules（用户指定）。后端所有校验、持久化和算法在服务端。无前端打包步骤。
后端：安装 backend/requirements.txt，设置 DATABASE_URL、REDIS_URL、JWT_SECRET、ARTIFACT_DIR 后在 backend/ 执行 `python -m migrations.initial`、`python -m scripts.seed_demo`、`uvicorn src.main:app`，另开 `python -m src.worker`。前端使用带 /api 反向代理的 Nginx，直接打开 index.html 不支持 API。

## 目录与工程约定
backend/src/{models,schemas,services,routes,controllers,middlewares,constants,config,utils}；backend/scripts/seed_demo.py；backend/migrations/initial.py。
frontend/src/{api,stores,types,components/common,hooks,pages,router,utils,constants}。
Role 枚举：admin/analyst/viewer；JobStatus：queued/running/completed/failed。后端 backend/src/constants/enums.py，前端 frontend/src/constants/enums.js；路由权限在 middlewares/auth.py；页面按相同角色控制操作入口。

## 环境变量与 Docker
COMPOSE_PROJECT_NAME=pick-paired；FRONTEND_PORT=18205；BACKEND_PORT=19205；DB_PORT=17705；DB_PASSWORD、JWT_SECRET 见 .env.example；SEED_DEMO 默认 true。共享命名产物卷供后端下载与 worker 写入，数据库/Redis 各持久卷。
恰好五服务均有健康检查。Redis 队列加 SQL 排队状态兜底；worker 按 next_run 生成本地收件箱文件。周期 60 秒至 30 天。分发结果为批准分析的固定快照；若需新数据重新计算并建立新计划。当前不连接邮件或企业聊天。
错误响应、请求编号、Redis 每 IP 每分钟 240 次限流、JWT 和数据库操作审计。演示密钥与密码仅供本机模拟；生产部署前配置独立密钥与账号。

## 采集站同步（本地可演练）
解决采集站持续补测/撤回下，手工整表导入漏更新的问题。导航新增「采集站同步」页，后端为可本地演练的源端模拟器与同步引擎。

**同步保证**
- 按站点独立保存游标（`sync_stations`）和逐记录版本/墓碑（`sync_record_states`）；只有更高版本才落库，重复或乱序不倒退，撤回墓碑不被旧消息复活。
- 游标过期（落在源端保留窗口外，或超过站点 TTL）时：先在按运行隔离的暂存区建全量副本（半成品对分析侧不可见）→ 接续期间增量（含复制进行中的新增与撤回）→ 追平后一次性原子发布为新的不可变 Dataset；每次发布留存游标、快照、应用/跳过计数与 SHA256 作为发布依据。
- 中断后恢复原运行与原游标位置，分段提交保证已确认进度不重做；相同进度重试是 noop，不新增批次；站点级互斥锁使并发同站不跳过变更、不重复发布。
- 无法证明事件连续（保留窗口内缺版本）时保留旧批次，运行标记 `gap` 并写明缺口范围；已发布批次不可变，已有分析继续绑定原批次。
- 同步页可查源端最新版本/本地游标/保留窗口、待追变更、源端镜像（含墓碑）、运行历史与发布依据。

**本地演练（无需 Docker/MySQL/Redis，使用临时 SQLite）**
```bash
cd backend
python -m tests.test_sync_engine   # 12 个引擎场景：乱序/撤回/全量副本/中断恢复/缺口/并发
python -m tests.test_sync_api      # 6 个 HTTP 端到端：权限、审计、批次不可见与不可变
python -m scripts.sync_demo        # 引导式剧情，打印每步游标、运行与发布依据
```
页面演练：登记采集站 → 在「源端演练」投递 upsert/withdraw、推进保留窗口制造过期 → 「执行同步」观察增量、全量副本、缺口与发布批次。

## 数据与验收
所有数据为固定随机种子生成的模拟业务数据，不能作为实际生产决策证据。项目独立算法、输入边界及业务局限详见 BUSINESS.md。测试与截图统一存放 ../../test-artifacts/gb-1006/，源码不含日志/截图/测试报告。
清理本项目演示数据：`docker compose down -v --remove-orphans`（会删除本项目命名卷）。

## License
MIT，见 LICENSE。
