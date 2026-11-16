import {sync} from '../api/entities.js';
import {workspace} from '../stores/workspace.js';
import {session} from '../stores/session.js';
import {heading} from '../components/common/shell.js';
import {table} from '../components/common/table.js';
import {esc,date,notify} from '../utils/dom.js';
import {submit} from '../hooks/forms.js';
import {SyncRunStatus,SyncRunMode,SyncOp} from '../constants/enums.js';

const canWrite=()=>session.user.role!=='viewer';

function runBadge(status){
 const tone=status==='published'?'published':status==='gap'?'failed':status==='interrupted'?'failed':status==='caught_up'?'caught':'';
 return '<span class="status '+tone+'">'+esc(SyncRunStatus[status]||status)+'</span>';
}

function progressCard(detail){
 const p=detail.progress;
 return '<div class="sync-progress'+(p.cursor_expired?' expired':'')+'">'
  +[['源端最新版本',p.source_head_version],['本地游标版本',p.local_cursor_version],
    ['保留窗口起点',p.retain_after_version],['待追事件',p.pending_event_count],
    ['待追版本跨度',p.pending_version_span]].map(([k,v])=>
   '<div><span>'+esc(k)+'</span><strong>'+esc(v)+'</strong></div>').join('')
  +'<div><span>游标状态</span><strong>'+(p.cursor_expired?'已过期·需全量副本':'有效·可增量接续')+'</strong></div></div>'
 +(p.cursor_expired?'<p class="warning">本地游标落在源端保留窗口之外：同步将先建立全量副本（暂存不可见），再接续期间增量，追平后一次性发布新批次。</p>':'');
}

function runArticle(r){
 const basis=r.publish_basis||{};
 return '<article class="run-card">'
  +'<div class="run-head"><strong>运行 #'+r.id+' · '+esc(SyncRunMode[r.mode]||r.mode)+'</strong>'+runBadge(r.status)+'</div>'
  +'<p class="run-line">起始游标 v'+r.start_cursor_version+' → 已应用 v'+r.applied_version
   +' / 源端 v'+r.source_head_version+' · 应用 '+r.applied_events+' 条 · 跳过 '+r.skipped_events+' 条'
   +(r.snapshot_version?' · 全量快照 v'+r.snapshot_version:'')+' · '+date(r.updated_at)+'</p>'
  +(r.gap_reason?'<p class="warning">缺口说明：'+esc(r.gap_reason)+(r.gap_from_version?'（自 v'+r.gap_from_version+' 起）':'')+'</p>':'')
  +(r.published_dataset_id?'<p class="run-line approval">发布依据：<a href="#/datasets">批次 #'+r.published_dataset_id+'</a> · '
   +esc(basis.record_count??'—')+' 条记录 · SHA256 '+esc((basis.checksum||'').slice(0,24))+'…</p>':'')
  +(r.note?'<small class="run-note">'+esc(r.note).replace(/\n/g,'<br>')+'</small>':'')
  +'</article>';
}

export async function syncPage(){
 workspace.syncStations=await sync.stations();
 const selectedId=workspace.syncSelected||workspace.syncStations[0]?.id;
 workspace.syncSelected=selectedId?Number(selectedId):null;
 const detail=selectedId?await sync.station(selectedId):null;
 workspace.syncDetail=detail;
 const stations=workspace.syncStations;
 const left='<section class="panel"><div class="section-title"><h2>采集站</h2><span>'+stations.length+' 个站点</span></div>'
  +'<div class="batch-list">'+(stations.map(s=>'<button class="batch-item'+(s.id===workspace.syncSelected?' active':'')+'" data-station="'+s.id+'"><strong>'+esc(s.name)+'</strong><span>'+esc(s.code)+' · 游标 v'+s.last_cursor_version+'</span><small>'+esc(s.endpoint)+'</small></button>').join('')||'<p class="empty">尚未登记采集站。</p>')+'</div>'
  +'<div class="section-title"><h2>登记采集站</h2></div>'
  +'<form id="station-form"><label>站点编码<input name="code" required minlength="2" placeholder="例如 ST-NL-01"></label><label>站点名称<input name="name" required minlength="2" placeholder="例如 北岭一号采集站"></label><button type="submit" class="primary" '+(canWrite()?'':'disabled')+'>登记站点</button></form></section>';
 let body;
 if(!detail){
  body='<section class="panel"><div class="empty">先在左侧登记一个采集站，再投递源端变更并执行同步演练。</div></section>';
 }else{
  const d=detail;
  body='<section class="panel"><div class="section-title"><h2>'+esc(d.station.name)+'</h2><span>'+esc(d.station.code)+'</span></div>'
   +progressCard(d)
   +'<div class="sync-actions"><button class="primary" id="run-sync" '+(canWrite()?'':'disabled')+'>执行同步 ↻</button>'
   +'<a class="button" href="#/datasets">查看已发布批次 ↗</a></div>'
   +(d.station.active_run_id?'<p class="hint">进行中的运行 #'+d.station.active_run_id+'：并发触发会复用该运行，不会重复建批次。</p>':'')
   +'</section>'
   // 本地演练：源端模拟器
   +'<section class="panel"><div class="section-title"><h2>源端演练（本地模拟器）</h2><span>真实部署替换为采集站 API</span></div>'
   +'<form id="event-form"><div class="parameter-grid"><label>操作类型<select name="op"><option value="upsert">新增/更新</option><option value="withdraw">撤回（墓碑）</option></select></label><label>记录自然键<input name="record_key" required placeholder="例如 OP-001"></label></div>'
   +'<label>记录内容 JSON（撤回可留空）<textarea name="payload" rows="4">'+esc(JSON.stringify({operator:'OP-001',team:'一组',before:410.2,after:378.6},null,2))+'</textarea></label>'
   +'<button type="submit" class="primary" '+(canWrite()?'':'disabled')+'>投递源端变更</button></form>'
   +'<form id="cursor-form" class="compact-form"><label>推进保留窗口（retain_after_version，制造游标过期）<input name="retain" type="number" min="0" value="'+d.station.retain_after_version+'"></label><button type="submit" class="button" '+(canWrite()?'':'disabled')+'>推进源端游标</button></form></section>'
   // 待追变更
   +'<section class="panel"><div class="section-title"><h2>待追变更</h2><span>'+d.progress.pending_event_count+' 条事件待同步</span></div><div id="pending-box">加载中…</div></section>'
   // 源端镜像（含墓碑）
   +'<section class="panel"><div class="section-title"><h2>源端当前镜像（含撤回墓碑）</h2><span>'+d.source_records.length+' 条</span></div>'
   +table([{key:'record_key',label:'自然键'},{key:'version',label:'版本'},{key:'op',label:'状态'},{key:'updated_at',label:'更新时间'}],
     d.source_records.map(r=>({record_key:r.record_key,version:r.version,op:SyncOp[r.op]||r.op,updated_at:date(r.updated_at)})))+'</section>'
   // 运行历史与发布依据
   +'<section class="panel"><div class="section-title"><h2>同步运行与发布依据</h2><span>最近 '+d.runs.length+' 次</span></div>'
   +'<div class="run-list">'+(d.runs.map(runArticle).join('')||'<p class="empty">还没有同步运行。</p>')+'</div></section>'
   +'<section class="panel"><div class="section-title"><h2>本地逐记录版本</h2><span>'+d.local_states.length+' 条（含墓碑）· 暂存区对分析侧不可见</span></div>'
   +table([{key:'record_key',label:'自然键'},{key:'version',label:'已确认版本'},{key:'op',label:'状态'}],
     d.local_states.map(r=>({record_key:r.record_key,version:r.version,op:SyncOp[r.op]||r.op})))+'</section>';
 }
 return heading('03 / STATION SYNC','采集站同步与批次发布','按站点保存游标与逐条版本；乱序不倒退、撤回不复活。游标过期先全量副本再接续增量，追平后一次性发布；无法证明连续时保留旧批次并说明缺口。')
  +'<div class="sync-grid">'+left+'<div>'+body+'</div></div>';
}

export function bindSync(refresh){
 document.querySelectorAll('[data-station]').forEach(b=>b.onclick=()=>{workspace.syncSelected=Number(b.dataset.station);refresh();});
 const stationForm=document.querySelector('#station-form');
 if(stationForm)submit(stationForm,async f=>{await sync.createStation({code:f.get('code'),name:f.get('name')});notify('采集站已登记');workspace.syncSelected=null;await refresh();});
 const detail=workspace.syncDetail;
 if(!detail)return;
 const id=detail.station.id;
 const runBtn=document.querySelector('#run-sync');
 if(runBtn)runBtn.onclick=async()=>{
  runBtn.disabled=true;
  try{const r=await sync.trigger(id);
   notify(r.status==='published'?('已发布批次 #'+r.published_dataset_id):r.status==='gap'?'存在缺口，已保留旧批次':SyncRunStatus[r.status]||r.status);
   await refresh();
  }catch(e){notify(e.message,true);runBtn.disabled=false;}
 };
 const eventForm=document.querySelector('#event-form');
 if(eventForm)submit(eventForm,async f=>{
  let payload={};const raw=(f.get('payload')||'').trim();
  if(raw){try{payload=JSON.parse(raw);}catch{throw new Error('记录内容不是合法 JSON');}}
  await sync.pushEvent(id,{op:f.get('op'),record_key:f.get('record_key'),payload});
  notify('源端变更已投递');await refresh();
 });
 const cursorForm=document.querySelector('#cursor-form');
 if(cursorForm)submit(cursorForm,async f=>{await sync.advance(id,Number(f.get('retain')));notify('源端保留窗口已推进');await refresh();});
 // 待追变更单独拉取，避免主表过大
 sync.pending(id).then(data=>{
  const box=document.querySelector('#pending-box');
  if(!box)return;
  const cols=[{key:'version',label:'版本'},{key:'op',label:'类型'},{key:'record_key',label:'记录自然键'},{key:'created_at',label:'源端时间'}];
  const rows=data.events.map(e=>({version:e.version,op:SyncOp[e.op]||e.op,record_key:e.record_key,created_at:date(e.created_at)}));
  box.innerHTML=table(cols,rows,'游标已追平，没有待追变更。');
 }).catch(e=>notify(e.message,true));
}
