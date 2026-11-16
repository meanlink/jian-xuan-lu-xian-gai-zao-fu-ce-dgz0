"""采集站同步的 HTTP 端到端演练（SQLite + FastAPI TestClient，无需 Redis/MySQL）。

运行：cd backend && python -m tests.test_sync_api
覆盖：站点登记、源端事件投递、游标推进、触发同步、进度/待追/发布依据查询、
权限控制（viewer 只读）、半成品批次不出现在数据集列表、审计留痕。
"""
import os
import tempfile
import unittest

_TMP_DB = os.path.join(tempfile.gettempdir(), 'sync_api_test.db')
if os.path.exists(_TMP_DB):
    os.remove(_TMP_DB)
os.environ.setdefault('DATABASE_URL', f'sqlite:///{_TMP_DB}')
os.environ.setdefault('JWT_SECRET', 'drill-test-secret')

from sqlalchemy import create_engine, event, select

from src.config import database as db_config

_engine = create_engine(os.environ['DATABASE_URL'],
                        connect_args={'check_same_thread': False, 'timeout': 30})


@event.listens_for(_engine, 'connect')
def _pragmas(conn, _):
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('PRAGMA busy_timeout=30000')
    cur.close()


db_config.engine = _engine
db_config.Session.configure(bind=_engine)

from fastapi.testclient import TestClient

from src.config.database import Base, Session
from src.main import app
from src.models import User, Dataset, Audit, SyncStation, SyncRun, SyncStaging
from src.utils.security import hash_password

Base.metadata.create_all(_engine)


def _seed_users():
    with Session() as db:
        if not db.scalar(select(User).where(User.email == 'admin@demo.local')):
            db.add(User(email='admin@demo.local', name='管理员', role='admin',
                        password=hash_password('Demo123!')))
        if not db.scalar(select(User).where(User.email == 'viewer@demo.local')):
            db.add(User(email='viewer@demo.local', name='观察员', role='viewer',
                        password=hash_password('Demo123!')))
        db.commit()


_seed_users()
client = TestClient(app)


def login(email):
    r = client.post('/auth/login', json={'email': email, 'password': 'Demo123!'})
    return r.json()['access_token']


AUTH = {'admin': None, 'viewer': None}


def headers(role):
    if AUTH[role] is None:
        AUTH[role] = login(f'{role}@demo.local')
    return {'Authorization': f'Bearer {AUTH[role]}'}


def payload(key, before=401.5, after=366.2):
    return {'operator': key, 'team': '二组', 'before': before, 'after': after}


class SyncApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        r = client.post('/sync/stations', headers=headers('admin'),
                        json={'code': 'API-01', 'name': '接口演练采集站',
                              'cursor_ttl_seconds': 600})
        cls.station_id = r.json()['id']

    def test_01_viewer_cannot_register_or_mutate(self):
        r = client.post('/sync/stations', headers=headers('viewer'),
                        json={'code': 'API-X', 'name': '无权站点'})
        self.assertEqual(r.status_code, 403)
        r = client.post(f'/sync/stations/{self.station_id}/sync',
                        headers=headers('viewer'))
        self.assertEqual(r.status_code, 403)

    def test_02_full_lifecycle_incremental(self):
        sid = self.station_id
        # 源端投递 6 条记录（含一次撤回）
        for i in range(1, 6):
            r = client.post(f'/sync/stations/{sid}/source/events',
                            headers=headers('admin'),
                            json={'op': 'upsert', 'record_key': f'OP-{i:03}',
                                  'payload': payload(f'OP-{i:03}')})
            self.assertEqual(r.status_code, 201)
        client.post(f'/sync/stations/{sid}/source/events',
                    headers=headers('admin'),
                    json={'op': 'withdraw', 'record_key': 'OP-001'})

        pending = client.get(f'/sync/stations/{sid}/pending',
                             headers=headers('admin')).json()
        self.assertEqual(pending['progress']['pending_event_count'], 6)
        self.assertFalse(pending['progress']['cursor_expired'])

        # 同步前数据集列表不含任何半成品
        before = {d['name'] for d in client.get('/datasets',
                                                headers=headers('admin')).json()}
        r = client.post(f'/sync/stations/{sid}/sync', headers=headers('admin'))
        self.assertEqual(r.status_code, 200)
        run = r.json()
        self.assertEqual(run['status'], 'published')
        self.assertEqual(run['applied_version'], 6)
        self.assertTrue(run['publish_basis']['checksum'])

        after = client.get('/datasets', headers=headers('admin')).json()
        names = {d['name'] for d in after}
        self.assertEqual(len(names - before), 1)
        published = next(d for d in after if d['id'] == run['published_dataset_id'])
        self.assertEqual(published['rows'], 4)  # 6 条事件，1 条撤回 → 4 条有效

        # 同进度重试：不新增批次
        r2 = client.post(f'/sync/stations/{sid}/sync', headers=headers('admin'))
        self.assertEqual(r2.json()['status'], 'caught_up')
        self.assertIsNone(r2.json()['published_dataset_id'])
        self.assertEqual(len(client.get('/datasets',
                                        headers=headers('admin')).json()), len(after))

    def test_03_expired_cursor_full_copy_flow(self):
        sid = self.station_id
        # 推进保留窗口越过本地游标 → 过期
        client.post(f'/sync/stations/{sid}/source/advance-cursor',
                    headers=headers('admin'), json={'retain_after_version': 7})
        # 全量期间的新增与撤回
        client.post(f'/sync/stations/{sid}/source/events',
                    headers=headers('admin'),
                    json={'op': 'upsert', 'record_key': 'OP-009',
                          'payload': payload('OP-009')})
        client.post(f'/sync/stations/{sid}/source/events',
                    headers=headers('admin'),
                    json={'op': 'withdraw', 'record_key': 'OP-002'})
        detail = client.get(f'/sync/stations/{sid}',
                            headers=headers('admin')).json()
        self.assertTrue(detail['progress']['cursor_expired'])

        r = client.post(f'/sync/stations/{sid}/sync', headers=headers('admin'))
        run = r.json()
        self.assertEqual(run['status'], 'published')
        self.assertEqual(run['mode'], 'full_then_incremental')
        ds = client.get(f"/datasets/{run['published_dataset_id']}",
                        headers=headers('admin')).json()
        keys = {row['operator'] for row in ds['records']}
        self.assertNotIn('OP-002', keys)
        self.assertIn('OP-009', keys)

        # 同步页可查源端进度、待追变更与发布依据
        detail = client.get(f'/sync/stations/{sid}',
                            headers=headers('admin')).json()
        self.assertEqual(detail['progress']['local_cursor_version'],
                         run['applied_version'])
        self.assertEqual(detail['progress']['pending_event_count'], 0)
        latest = detail['runs'][0]
        self.assertEqual(latest['publish_basis']['station_code'], 'API-01')
        self.assertIn('checksum', latest['publish_basis'])
        # 源端镜像中保留撤回墓碑可供核对
        self.assertIn('withdraw', {r['op'] for r in detail['source_records']})

    def test_04_lock_conflict_returns_409(self):
        # 串行环境下锁已释放；构造一个残留锁验证冲突响应
        with Session() as db:
            st = db.get(SyncStation, self.station_id)
            st.lock_owner = 'stranger'
            st.lock_until = 9_999_999_999.0
            st.active_run_id = None
            db.commit()
        r = client.post(f'/sync/stations/{self.station_id}/sync',
                        headers=headers('admin'))
        self.assertEqual(r.status_code, 409)
        with Session() as db:
            st = db.get(SyncStation, self.station_id)
            st.lock_owner, st.lock_until = None, None
            db.commit()

    def test_05_audit_trail_records_sync_actions(self):
        audits = client.get('/audits', headers=headers('admin')).json()
        actions = {a['action'] for a in audits}
        self.assertIn('sync.station.create', actions)
        self.assertIn('sync.source.event', actions)
        self.assertIn('sync.run.finish', actions)
        self.assertIn('sync.source.advance', actions)

    def test_06_immutable_batches_keep_job_binding(self):
        # 对已发布批次创建分析任务，再做一轮同步，任务绑定不变
        datasets = client.get('/datasets', headers=headers('admin')).json()
        target = max(datasets, key=lambda d: d['id'])
        job = client.post('/jobs', headers=headers('admin'),
                          json={'name': '同步批次核验', 'dataset_id': target['id'],
                                'params': {'team': '全部', 'confidence': '0.95'}})
        self.assertEqual(job.status_code, 201)
        job_id = job.json()['id']
        client.post(f'/sync/stations/{self.station_id}/source/events',
                    headers=headers('admin'),
                    json={'op': 'upsert', 'record_key': 'OP-010',
                          'payload': payload('OP-010')})
        client.post(f'/sync/stations/{self.station_id}/sync',
                    headers=headers('admin'))
        bound = client.get(f'/jobs/{job_id}', headers=headers('admin')).json()
        self.assertEqual(bound['dataset_id'], target['id'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
