import {api} from './client.js';
export const datasets={list:()=>api('/datasets'),get:id=>api('/datasets/'+id),create:data=>api('/datasets',{method:'POST',body:JSON.stringify(data)})};
export const jobs={list:()=>api('/jobs'),get:id=>api('/jobs/'+id),create:data=>api('/jobs',{method:'POST',body:JSON.stringify(data)}),approve:id=>api('/jobs/'+id+'/approve',{method:'POST'})};
export const reports={list:()=>api('/reports'),create:data=>api('/reports',{method:'POST',body:JSON.stringify(data)}),toggle:id=>api('/reports/'+id+'/toggle',{method:'POST'}),deliveries:()=>api('/deliveries')};
export const sync={
 stations:()=>api('/sync/stations'),
 createStation:data=>api('/sync/stations',{method:'POST',body:JSON.stringify(data)}),
 station:id=>api('/sync/stations/'+id),
 trigger:id=>api('/sync/stations/'+id+'/sync',{method:'POST'}),
 runs:id=>api('/sync/stations/'+id+'/runs'),
 pending:id=>api('/sync/stations/'+id+'/pending'),
 pushEvent:(id,data)=>api('/sync/stations/'+id+'/source/events',{method:'POST',body:JSON.stringify(data)}),
 advance:(id,retain_after_version)=>api('/sync/stations/'+id+'/source/advance-cursor',{method:'POST',body:JSON.stringify({retain_after_version})}),
 sourceEvents:id=>api('/sync/stations/'+id+'/source/events'),
};
