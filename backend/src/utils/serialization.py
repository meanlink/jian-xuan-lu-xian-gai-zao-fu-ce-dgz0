import hashlib, json
def checksum(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
def dataset_dict(d):
    return {'id':d.id, 'name':d.name, 'source':d.source, 'rows':len(d.records), 'checksum':d.checksum, 'created_at':d.created_at}
def job_dict(j):
    return {k:getattr(j,k) for k in ['id','name','dataset_id','params','status','result','error','approved','created_at','finished_at']}
def report_dict(r):
    return {k:getattr(r,k) for k in ['id','name','job_id','destination','interval_seconds','next_run','enabled']}
def delivery_dict(d):
    return {k:getattr(d,k) for k in ['id','report_id','created_at','checksum','filename','destination']}
def station_dict(s):
    return {k:getattr(s,k) for k in ['id','code','name','endpoint','cursor_ttl_seconds',
            'retain_after_version','last_cursor_version','last_synced_at','active_run_id']}
def run_dict(r):
    return {k:getattr(r,k) for k in ['id','station_id','mode','status','start_cursor_version',
            'snapshot_version','applied_version','source_head_version','applied_events',
            'skipped_events','gap_from_version','gap_reason','published_dataset_id',
            'publish_basis','owner','note','created_at','updated_at']}
def source_event_dict(e):
    return {'id':e.id,'version':e.version,'op':e.op,'record_key':e.record_key,
            'payload':e.payload,'created_at':e.created_at}
def source_record_dict(r):
    return {'record_key':r.record_key,'version':r.version,'op':r.op,
            'payload':r.payload,'updated_at':r.updated_at}
def record_state_dict(r):
    return {'record_key':r.record_key,'version':r.version,'op':r.op,
            'payload':r.payload,'updated_at':r.updated_at}
