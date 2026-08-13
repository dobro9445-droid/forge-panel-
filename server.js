'use strict';
const express=require('express'),http=require('http'),path=require('path'),fs=require('fs'),fsp=fs.promises,
crypto=require('crypto'),os=require('os'),{spawn,execFile}=require('child_process'),{WebSocketServer}=require('ws'),
pidusage=require('pidusage'),multer=require('multer'),archiver=require('archiver');
const ROOT=__dirname,DATA_DIR=path.join(ROOT,'data'),SERVERS_ROOT=path.join(ROOT,'servers'),
BACKUP_ROOT=path.join(ROOT,'backups'),DB_FILE=path.join(DATA_DIR,'panel.json'),IS_WIN=process.platform==='win32';
for(const d of[DATA_DIR,SERVERS_ROOT,BACKUP_ROOT])fs.mkdirSync(d,{recursive:true});
const uid=p=>p+crypto.randomBytes(4).toString('hex');
const now=()=>Date.now();
const hashPass=(p,salt)=>crypto.scryptSync(p,salt,32).toString('hex');
function parseCookies(req){const o={};(req.headers.cookie||'').split(';').forEach(c=>{const i=c.indexOf('=');if(i>-1)o[c.slice(0,i).trim()]=decodeURIComponent(c.slice(i+1));});return o;}
function slug(s){return(s||'').trim().toLowerCase().replace(/[^a-z0-9а-яё]+/gi,'-').replace(/^-+|-+$/g,'').slice(0,40)||'server';}
function defaultDB(){
  const salt=crypto.randomBytes(8).toString('hex');
  const initialPass=process.env.ADMIN_PASSWORD||'admin';
  return{settings:{name:'FORGE',port:8088},users:[{u:'admin',salt,h:hashPass(initialPass,salt)}],sessions:{},servers:[],templates:[],activity:[]};
}
function loadDB(){try{return JSON.parse(fs.readFileSync(DB_FILE,'utf8'));}catch{return defaultDB();}}
const db=loadDB();db.templates=db.templates||[];
let saveT=null;
function saveDB(){clearTimeout(saveT);saveT=setTimeout(()=>{try{fs.writeFileSync(DB_FILE,JSON.stringify(db,null,1));}catch(e){console.error('DB:',e.message);}},250);}
function act(user,a,detail){db.activity.unshift({ts:now(),user,a,detail});db.activity=db.activity.slice(0,200);saveDB();}
function newToken(user){const t=crypto.randomBytes(24).toString('hex');db.sessions[t]={user,exp:now()+7*864e5};saveDB();return t;}
const tokenUser=t=>{const s=db.sessions[t];return s&&s.exp>=now()?s.user:null;};
const RT=new Map();
function R(id){if(!RT.has(id))RT.set(id,{child:null,status:'offline',lines:[],cpu:[],ram:[],startedAt:0,stopping:false,pendingRestart:false,crashes:0,runTimer:null,killTimer:null});return RT.get(id);}
const srvDir=s=>path.join(SERVERS_ROOT,s.dir);
const findSrv=id=>db.servers.find(s=>s.id===id);
function pushLog(id,line,stream='out'){const rt=R(id);rt.lines.push({ts:now(),s:stream,l:line.slice(0,2000)});if(rt.lines.length>600)rt.lines.splice(0,rt.lines.length-600);bc({t:'log',id,ts:now(),line,stream});}
function killTree(pid,sig){if(!pid)return;if(IS_WIN){const a=['/pid',String(pid),'/T'];if(sig==='SIGKILL')a.push('/F');execFile('taskkill',a,()=>{});}else{try{process.kill(-pid,sig);}catch{try{process.kill(pid,sig);}catch{}}}}
function spawnServer(srv){
  const rt=R(srv.id);if(rt.child)return;
  if(!srv.command||!srv.command.trim()){pushLog(srv.id,'Не задана команда запуска','sys');return;}
  rt.stopping=false;rt.pendingRestart=false;rt.status='starting';
  pushLog(srv.id,'▶ Запуск: '+srv.command,'sys');
  let child;
  try{child=spawn(srv.command,{shell:true,cwd:srvDir(srv),env:{...process.env,...(srv.env||{})},detached:!IS_WIN,stdio:['pipe','pipe','pipe']});}
  catch(e){pushLog(srv.id,'Ошибка spawn: '+e.message,'err');rt.status='offline';bcState();return;}
  rt.child=child;rt.startedAt=now();
  const feed=stream=>d=>String(d).split('\n').forEach(l=>{if(!l.trim())return;if(rt.status==='starting'){rt.status='running';bcState();}pushLog(srv.id,l,stream);});
  child.stdout.on('data',feed('out'));child.stderr.on('data',feed('err'));
  child.on('error',e=>pushLog(srv.id,'Ошибка процесса: '+e.message,'err'));
  child.on('exit',(code,sig)=>{
    clearTimeout(rt.runTimer);clearTimeout(rt.killTimer);
    const wasStop=rt.stopping,pend=rt.pendingRestart;
    rt.child=null;rt.status='offline';rt.stopping=false;rt.pendingRestart=false;rt.startedAt=0;
    pushLog(srv.id,`■ Процесс завершён (код ${code}${sig?', сигнал '+sig:''})`,'sys');
    if(pend)setTimeout(()=>spawnServer(srv),600);
    else if(!wasStop&&srv.restartOnCrash&&code!==0){
      rt.crashes=(rt.crashes||0)+1;
      if(rt.crashes<=3){pushLog(srv.id,`Авторестарт через 3 сек (попытка ${rt.crashes}/3)…`,'sys');setTimeout(()=>{if(!R(srv.id).child)spawnServer(srv);},3000);}
      else{pushLog(srv.id,'Crash-loop: автоперезапуск отключён','err');rt.crashes=0;}
    }
    bcState();
  });
  rt.runTimer=setTimeout(()=>{if(rt.child&&rt.status==='starting'){rt.status='running';bcState();}},1500);
  bcState();
}
function stopServer(srv,hard=false){
  const rt=R(srv.id);if(!rt.child)return;
  rt.stopping=true;rt.pendingRestart=false;rt.status='stopping';bcState();
  if(hard){pushLog(srv.id,'⚡ SIGKILL','sys');killTree(rt.child.pid,'SIGKILL');return;}
  pushLog(srv.id,'Остановка (SIGTERM)…','sys');killTree(rt.child.pid,'SIGTERM');
  rt.killTimer=setTimeout(()=>{if(rt.child){pushLog(srv.id,'Таймаут остановки — SIGKILL','sys');killTree(rt.child.pid,'SIGKILL');}},8000);
}
function power(srv,action){const rt=R(srv.id);
  if(action==='start'){if(!rt.child)spawnServer(srv);}
  else if(action==='stop')stopServer(srv);
  else if(action==='kill')stopServer(srv,true);
  else if(action==='restart'){if(rt.child){rt.pendingRestart=true;stopServer(srv);}else spawnServer(srv);}}
function sendCommand(id,cmd){const rt=R(id);if(!rt.child||!rt.child.stdin.writable)return false;rt.child.stdin.write(cmd+'\n');pushLog(id,'> '+cmd,'cmd');return true;}
function psTable(){return new Promise((res,rej)=>execFile('ps',['-axo','pid=,ppid=,pcpu=,rss='],(e,out)=>{if(e)return rej(e);const rows={};out.trim().split('\n').forEach(l=>{const p=l.trim().split(/\s+/);if(p.length>=4)rows[p[0]]={pid:+p[0],ppid:+p[1],cpu:+p[2]||0,rss:+p[3]||0};});res(rows);}));}
function treeStats(table,root){let cpu=0,mem=0;const seen=new Set(),q=[root];while(q.length){const pid=q.pop();if(seen.has(pid))continue;seen.add(pid);const r=table[pid];if(!r)continue;cpu+=r.cpu;mem+=r.rss*1024;for(const k in table)if(table[k].ppid===pid)q.push(+k);}return{cpu,mem};}
setInterval(async()=>{
  let table=null;if(!IS_WIN){try{table=await psTable();}catch{}}
  for(const srv of db.servers){
    const rt=R(srv.id);
    if(rt.child&&rt.status==='running'&&now()-rt.startedAt>60000)rt.crashes=0;
    let cpu=0,mem=0;
    if(rt.child){if(table)({cpu,mem}=treeStats(table,rt.child.pid));else{try{const s=await pidusage(rt.child.pid);cpu=s.cpu||0;mem=s.memory||0;}catch{}}}
    rt.cpu.push(+cpu.toFixed(1));rt.ram.push(Math.round(mem/1048576));
    if(rt.cpu.length>120){rt.cpu.shift();rt.ram.shift();}
  }
  bcState();
},2000);
setInterval(()=>{
  let dirty=false;
  for(const srv of db.servers)for(const t of srv.tasks||[]){
    if(!t.enabled)continue;
    if(!t.lastRun){t.lastRun=now();dirty=true;continue;}
    if(now()-t.lastRun>=t.every*60000){t.lastRun=now();dirty=true;pushLog(srv.id,`⏰ Задача «${t.name}»`,'sys');
      if(t.action==='command')sendCommand(srv.id,t.payload||'');else power(srv,t.action);}
  }
  if(dirty)saveDB();
},15000);
const httpServer=http.createServer();
const wss=new WebSocketServer({server:httpServer,path:'/ws'});
function bc(obj){const s=JSON.stringify(obj);for(const c of wss.clients)if(c.readyState===1&&c.isAuth)c.send(s);}
function pubState(){return db.servers.map(s=>{const rt=R(s.id);return{...s,status:rt.status,startedAt:rt.startedAt,cpu:rt.cpu.slice(-60),ram:rt.ram.slice(-60)};});}
function bcState(){bc({t:'state',servers:pubState()});}
wss.on('connection',ws=>{ws.isAuth=false;ws.on('message',raw=>{let m;try{m=JSON.parse(raw);}catch{return;}
  if(m.t==='auth'){if(tokenUser(m.token)){ws.isAuth=true;ws.send(JSON.stringify({t:'state',servers:pubState()}));}else ws.close();}
  else if(ws.isAuth&&m.t==='cmd')sendCommand(m.id,m.cmd||'');});});
const app=express();
app.disable('x-powered-by');
app.use(express.json({limit:'4mb'}));
app.use(express.static(path.join(ROOT,'public')));
const auth=(req,res,next)=>{const u=tokenUser(parseCookies(req).tok);if(!u)return res.status(401).json({error:'Не авторизован'});req.user=u;next();};
const ah=fn=>(req,res)=>Promise.resolve(fn(req,res)).catch(e=>res.status(500).json({error:e.message}));
function safeJoin(root,rel){const p=path.resolve(root,'.'+(rel?'/' +String(rel).replace(/\\/g,'/'):''));if(p!==root&&!p.startsWith(root+path.sep))throw new Error('Путь вне директории сервера');return p;}
app.get('/healthz',(req,res)=>res.json({ok:1,uptime:process.uptime()}));
app.post('/api/login',ah(async(req,res)=>{
  const{u,p}=req.body||{};const user=db.users.find(x=>x.u===u);
  if(!user||hashPass(p||'',user.salt)!==user.h)return res.status(401).json({error:'Неверный логин или пароль'});
  const t=newToken(user.u);
  res.setHeader('Set-Cookie','tok='+t+'; Path=/; Max-Age=604800; SameSite=Lax');
  act(user.u,'Вход','Успешная авторизация');res.json({ok:1});
}));
app.post('/api/logout',auth,(req,res)=>{for(const k in db.sessions)if(db.sessions[k].user===req.user)delete db.sessions[k];saveDB();res.setHeader('Set-Cookie','tok=; Path=/; Max-Age=0');res.json({ok:1});});
app.get('/api/state',auth,(req,res)=>res.json({user:req.user,settings:db.settings,sys:{platform:process.platform,node:process.version,cores:os.cpus().length,totalMem:os.totalmem()},servers:pubState(),templates:db.templates}));
app.get('/api/servers/:id/console',auth,ah(async(req,res)=>res.json({lines:R(req.params.id).lines.slice(-250)})));
app.post('/api/servers',auth,ah(async(req,res)=>{
  const{name,command,port,autoStart}=req.body||{};
  if(!name||!name.trim())throw new Error('Укажите имя сервера');
  const base=slug(name);let dir=base,i=1;
  while(fs.existsSync(path.join(SERVERS_ROOT,dir)))dir=base+'-'+i++;
  await fsp.mkdir(path.join(SERVERS_ROOT,dir));
  const srv={id:uid('srv_'),name:name.trim(),dir,command:command||'',env:{},tasks:[],port:+port||25565,autoStart:!!autoStart,restartOnCrash:false,limits:{cpu:100,ram:2048},createdAt:now()};
  db.servers.push(srv);saveDB();act(req.user,'Создан сервер',srv.name);res.json(srv);
}));
app.patch('/api/servers/:id',auth,ah(async(req,res)=>{
  const srv=findSrv(req.params.id);if(!srv)throw new Error('Сервер не найден');const b=req.body||{};
  for(const k of['name','command','autoStart','restartOnCrash'])if(b[k]!==undefined)srv[k]=b[k];
  if(b.env&&typeof b.env==='object'){srv.env={};for(const k in b.env)srv.env[k]=String(b.env[k]);}
  if(b.limits)srv.limits={cpu:+b.limits.cpu||100,ram:+b.limits.ram||1024};
  if(b.port)srv.port=+b.port;
  saveDB();act(req.user,'Настройки сервера',srv.name);res.json(srv);
}));
app.delete('/api/servers/:id',auth,ah(async(req,res)=>{
  const srv=findSrv(req.params.id);if(!srv)throw new Error('Сервер не найден');
  const rt=R(srv.id);if(rt.child){rt.stopping=true;killTree(rt.child.pid,'SIGKILL');}
  if(req.query.wipe==='1')await fsp.rm(srvDir(srv),{recursive:true,force:true});
  await fsp.rm(path.join(BACKUP_ROOT,srv.id),{recursive:true,force:true}).catch(()=>{});
  db.servers=db.servers.filter(s=>s.id!==srv.id);RT.delete(srv.id);saveDB();
  act(req.user,'Удалён сервер',srv.name);res.json({ok:1});
}));
app.post('/api/servers/:id/power',auth,ah(async(req,res)=>{
  const srv=findSrv(req.params.id);if(!srv)throw new Error('Сервер не найден');
  const a=req.body.action;if(!['start','stop','restart','kill'].includes(a))throw new Error('Неизвестное действие');
  power(srv,a);act(req.user,'Питание: '+a,srv.name);res.json({ok:1});
}));
app.post('/api/servers/:id/command',auth,ah(async(req,res)=>{if(!sendCommand(req.params.id,String(req.body.command||'')))throw new Error('Сервер не запущен');res.json({ok:1});}));
app.get('/api/servers/:id/files',auth,ah(async(req,res)=>{
  const srv=findSrv(req.params.id);if(!srv)throw new Error('404');
  const rel=req.query.path||'';const abs=safeJoin(srvDir(srv),rel);
  const items=await fsp.readdir(abs,{withFileTypes:true});const out=[];
  for(const it of items){const st=await fsp.stat(path.join(abs,it.name)).catch(()=>null);out.push({name:it.name,dir:it.isDirectory(),size:st?st.size:0,mtime:st?st.mtimeMs:0});}
  out.sort((a,b)=>(b.dir-a.dir)||a.name.localeCompare(b.name));res.json({path:rel,items:out});
}));
app.get('/api/servers/:id/file',auth,ah(async(req,res)=>{
  const srv=findSrv(req.params.id);const abs=safeJoin(srvDir(srv),req.query.path);
  const st=await fsp.stat(abs);if(st.size>400*1024)throw new Error('Файл слишком большой для редактора (>400 КБ)');
  const buf=await fsp.readFile(abs);if(buf.slice(0,512).includes(0))throw new Error('Бинарный файл — используйте скачивание');
  res.json({content:buf.toString('utf8')});
}));
app.post('/api/servers/:id/file',auth,ah(async(req,res)=>{const srv=findSrv(req.params.id);const abs=safeJoin(srvDir(srv),req.body.path);await fsp.mkdir(path.dirname(abs),{recursive:true});await fsp.writeFile(abs,String(req.body.content??''),'utf8');res.json({ok:1});}));
app.post('/api/servers/:id/files/mkdir',auth,ah(async(req,res)=>{const srv=findSrv(req.params.id);await fsp.mkdir(safeJoin(srvDir(srv),req.body.path));res.json({ok:1});}));
app.post('/api/servers/:id/files/touch',auth,ah(async(req,res)=>{const srv=findSrv(req.params.id);const abs=safeJoin(srvDir(srv),req.body.path);await fsp.mkdir(path.dirname(abs),{recursive:true});if(!fs.existsSync(abs))await fsp.writeFile(abs,'');res.json({ok:1});}));
app.post('/api/servers/:id/files/rename',auth,ah(async(req,res)=>{const srv=findSrv(req.params.id);await fsp.rename(safeJoin(srvDir(srv),req.body.from),safeJoin(srvDir(srv),req.body.to));res.json({ok:1});}));
app.post('/api/servers/:id/files/delete',auth,ah(async(req,res)=>{const srv=findSrv(req.params.id);await fsp.rm(safeJoin(srvDir(srv),req.body.path),{recursive:true,force:true});res.json({ok:1});}));
app.get('/api/servers/:id/files/download',auth,ah(async(req,res)=>{const srv=findSrv(req.params.id);const abs=safeJoin(srvDir(srv),req.query.path);res.download(abs,path.basename(abs));}));
const up=multer({limits:{fileSize:200*1048576},storage:multer.diskStorage({
  destination(req,file,cb){try{const srv=findSrv(req.params.id);const dir=safeJoin(srvDir(srv),req.body.dir||'');fs.mkdirSync(dir,{recursive:true});cb(null,dir);}catch(e){cb(e);}},
  filename(req,file,cb){const name=Buffer.from(file.originalname,'latin1').toString('utf8');cb(null,path.basename(name).replace(/[\x00-\x1f]/g,''));}
})});
app.post('/api/servers/:id/files/upload',auth,(req,res)=>{up.single('file')(req,res,e=>e?res.status(500).json({error:e.message}):res.json({ok:1}));});
app.get('/api/servers/:id/backups',auth,ah(async(req,res)=>{
  const dir=path.join(BACKUP_ROOT,req.params.id);if(!fs.existsSync(dir))return res.json([]);
  const out=[];for(const f of await fsp.readdir(dir)){if(!f.endsWith('.zip'))continue;const st=await fsp.stat(path.join(dir,f));out.push({file:f,size:st.size,ts:st.mtimeMs});}
  res.json(out.sort((a,b)=>b.ts-a.ts));
}));
app.post('/api/servers/:id/backups',auth,ah(async(req,res)=>{
  const srv=findSrv(req.params.id);if(!srv)throw new Error('404');
  const outDir=path.join(BACKUP_ROOT,srv.id);await fsp.mkdir(outDir,{recursive:true});
  const file=`${slug(srv.name)}-${new Date().toISOString().replace(/[:.]/g,'-').slice(0,19)}.zip`;
  const out=path.join(outDir,file);
  await new Promise((ok,err)=>{const ws=fs.createWriteStream(out);const arc=archiver('zip',{zlib:{level:6}});arc.on('error',err);ws.on('close',ok);ws.on('error',err);arc.pipe(ws);arc.directory(srvDir(srv),false);arc.finalize();});
  act(req.user,'Бэкап создан',`${srv.name} → ${file}`);res.json({ok:1,file});
}));
app.get('/api/backups/:srv/:file',auth,ah(async(req,res)=>{if(!/^[\w.-]+\.zip$/.test(req.params.file))throw new Error('Неверное имя');res.download(path.join(BACKUP_ROOT,req.params.srv,req.params.file));}));
app.delete('/api/backups/:srv/:file',auth,ah(async(req,res)=>{if(!/^[\w.-]+\.zip$/.test(req.params.file))throw new Error('Неверное имя');await fsp.rm(path.join(BACKUP_ROOT,req.params.srv,req.params.file),{force:true});res.json({ok:1});}));
app.post('/api/servers/:id/tasks',auth,ah(async(req,res)=>{
  const srv=findSrv(req.params.id);srv.tasks=srv.tasks||[];
  const t={id:uid('tsk_'),name:req.body.name||'Задача',every:Math.max(1,+req.body.every||60),action:req.body.action||'restart',payload:req.body.payload||'',enabled:true,lastRun:0};
  srv.tasks.push(t);saveDB();res.json(t);
}));
app.patch('/api/servers/:id/tasks/:tid',auth,ah(async(req,res)=>{const srv=findSrv(req.params.id);const t=(srv.tasks||[]).find(x=>x.id===req.params.tid);if(!t)throw new Error('Задача не найдена');Object.assign(t,req.body,{id:t.id});saveDB();res.json(t);}));
app.delete('/api/servers/:id/tasks/:tid',auth,ah(async(req,res)=>{const srv=findSrv(req.params.id);srv.tasks=(srv.tasks||[]).filter(x=>x.id!==req.params.tid);saveDB();res.json({ok:1});}));
app.post('/api/servers/:id/tasks/:tid/run',auth,ah(async(req,res)=>{
  const srv=findSrv(req.params.id);const t=(srv.tasks||[]).find(x=>x.id===req.params.tid);if(!t)throw new Error('Задача не найдена');
  if(t.action==='command')sendCommand(srv.id,t.payload||'');else power(srv,t.action);res.json({ok:1});
}));
app.post('/api/templates',auth,ah(async(req,res)=>{
  const b=req.body||{};if(!b.name||!b.name.trim())throw new Error('Укажите название игры');
  const t={id:uid('tpl_'),name:b.name.trim().slice(0,40),icon:String(b.icon||'🎮').slice(0,8),command:String(b.command||'').slice(0,500),port:+b.port||27015,note:String(b.note||'').slice(0,120)};
  db.templates.push(t);saveDB();act(req.user,'Шаблон игры',t.name);res.json(t);
}));
app.patch('/api/templates/:tid',auth,ah(async(req,res)=>{
  const t=db.templates.find(x=>x.id===req.params.tid);if(!t)throw new Error('Шаблон не найден');const b=req.body||{};
  for(const k of['name','icon','command','note'])if(b[k]!==undefined)t[k]=String(b[k]);
  if(b.port)t.port=+b.port;saveDB();res.json(t);
}));
app.delete('/api/templates/:tid',auth,ah(async(req,res)=>{db.templates=db.templates.filter(x=>x.id!==req.params.tid);saveDB();res.json({ok:1});}));
app.get('/api/activity',auth,(req,res)=>res.json(db.activity.slice(0,100)));
app.post('/api/settings',auth,ah(async(req,res)=>{
  if(req.body.name)db.settings.name=String(req.body.name).slice(0,30);
  if(req.body.port&&+req.body.port!==db.settings.port){db.settings.port=+req.body.port;saveDB();act(req.user,'Настройки панели','Порт → '+db.settings.port);
    setTimeout(()=>{httpServer.close(()=>{});httpServer.listen(db.settings.port);},300);}
  else saveDB();
  res.json(db.settings);
}));
app.post('/api/password',auth,ah(async(req,res)=>{
  const user=db.users.find(x=>x.u===req.user);
  if(hashPass(req.body.old||'',user.salt)!==user.h)throw new Error('Неверный текущий пароль');
  if((req.body.neu||'').length<4)throw new Error('Минимум 4 символа');
  user.salt=crypto.randomBytes(8).toString('hex');user.h=hashPass(req.body.neu,user.salt);
  db.sessions={};saveDB();res.setHeader('Set-Cookie','tok=; Path=/; Max-Age=0');res.json({ok:1});
}));
httpServer.on('request',app);
const PORT=process.env.PORT||db.settings.port||8088;
httpServer.listen(PORT,()=>{
  console.log('=============================================');
  console.log(`  FORGE панель:  http://localhost:${PORT}`);
  console.log(process.env.ADMIN_PASSWORD?'  Логин: admin (пароль из $ADMIN_PASSWORD)':'  Логин: admin  Пароль: admin (смените!)');
  console.log('=============================================');
  setTimeout(()=>db.servers.filter(s=>s.autoStart).forEach(s=>{pushLog(s.id,'Автозапуск при старте панели','sys');spawnServer(s);}),800);
});
process.on('SIGINT',()=>{for(const s of db.servers){const rt=R(s.id);if(rt.child)killTree(rt.child.pid,'SIGKILL');}process.exit(0);});
