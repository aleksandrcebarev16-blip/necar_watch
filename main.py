import asyncio
import json
import re
import secrets
from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()
rooms = defaultdict(set)
room_state = {}
MAX_PEOPLE = 2

HTML = r"""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>necar watch</title>
<style>
:root{color-scheme:dark;--bg:#0b0b0f;--panel:#15151c;--line:#292934;--text:#f4f4f7;--muted:#9999aa;--accent:#a78bfa}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px system-ui,-apple-system,sans-serif}
main{max-width:1100px;margin:0 auto;padding:22px 14px;display:grid;gap:14px}
header{display:flex;align-items:center;justify-content:space-between;gap:12px}
.logo{font-weight:800;letter-spacing:-.6px;font-size:22px}.logo span{color:var(--accent)}
.small{color:var(--muted);font-size:12px}
.topline{display:flex;gap:8px;align-items:center}
input,button{font:inherit;border-radius:11px;border:1px solid var(--line);padding:11px 12px;background:#111117;color:var(--text)}
button{cursor:pointer;background:var(--accent);border-color:var(--accent);color:#171321;font-weight:700}
button.secondary{background:var(--panel);color:var(--text);border-color:var(--line)}
.urlrow{display:flex;gap:8px}.urlrow input{flex:1;min-width:0}
.layout{display:grid;grid-template-columns:minmax(0,1.7fr) minmax(250px,.8fr);gap:14px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:16px;overflow:hidden;min-width:0}
.player{aspect-ratio:16/9;background:#000;display:flex;align-items:center;justify-content:center}
.player iframe,.player video{width:100%;height:100%;border:0;background:#000}
.placeholder{color:#777;text-align:center;padding:20px}
.player-tools{padding:10px;display:flex;gap:8px;flex-wrap:wrap;border-top:1px solid var(--line)}
.player-tools input{width:100px}.chat{display:flex;flex-direction:column;min-height:390px;max-height:560px}
.chathead{padding:14px;border-bottom:1px solid var(--line);font-weight:700;display:flex;justify-content:space-between}
.messages{padding:12px;display:flex;flex-direction:column;gap:9px;overflow:auto;flex:1}
.msg{background:#20202a;padding:9px 11px;border-radius:11px;overflow-wrap:anywhere}
.msg .who{font-size:11px;color:var(--accent);margin-bottom:3px}
.chatrow{display:flex;gap:7px;padding:10px;border-top:1px solid var(--line)}.chatrow input{flex:1;min-width:0}
.status{font-size:12px;color:var(--muted)}.roomline{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
.note{color:var(--muted);font-size:12px;line-height:1.45}
@media(max-width:760px){.layout{grid-template-columns:1fr}.chat{min-height:300px;max-height:420px}main{padding:14px 10px}.logo{font-size:20px}}
</style>
</head>
<body>
<main>
<header><div class="logo">necar <span>watch</span></div><div class="topline"><span id="people" class="small">Подключение…</span></div></header>
<div class="roomline"><div><div style="font-weight:700">Ваша комната</div><div class="small">Отправьте ссылку партнёру</div></div><button id="copy" class="secondary">Копировать приглашение</button></div>
<div class="urlrow"><input id="url" placeholder="Вставьте ссылку на видео или страницу"><button id="load">Открыть</button></div>
<div class="layout">
<section class="card">
<div id="player" class="player"><div class="placeholder">Вставьте ссылку, чтобы начать просмотр</div></div>
<div class="player-tools">
<button class="secondary" id="play">▶ / ⏸</button>
<button class="secondary" id="back">−10 сек</button><button class="secondary" id="forward">+10 сек</button>
<span class="note" style="align-self:center">Управление синхронизируется там, где сайт это поддерживает.</span>
</div>
</section>
<section class="card chat">
<div class="chathead"><span>Чат</span><span id="chatstatus" class="small">соединение…</span></div>
<div id="messages" class="messages"></div>
<form id="chatform" class="chatrow"><input id="message" maxlength="1000" placeholder="Написать сообщение…" autocomplete="off"><button>➤</button></form>
</section>
</div>
<div class="note">Поддерживаются прямые ссылки на видеофайлы, YouTube и сайты, разрешающие встраивание. Некоторые сайты блокируют открытие внутри других страниц. В таком случае откройте видео на исходном сайте в отдельной вкладке: автоматическая синхронизация стороннего плеера может быть недоступна.</div>
</main>
<script>
const room = location.pathname.split('/').pop();
const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:';
let ws, currentType = '', nativeVideo = null, ytPlayer = null, syncTimer = null, lastRemote = false;
const $ = id => document.getElementById(id);
function send(data){if(ws && ws.readyState===1) ws.send(JSON.stringify(data))}
function addMessage(name,text,system=false){
 const box=document.createElement('div');box.className='msg';
 if(!system){const who=document.createElement('div');who.className='who';who.textContent=name;box.appendChild(who)}
 const body=document.createElement('div');body.textContent=text;box.appendChild(body);$('messages').appendChild(box);$('messages').scrollTop=$('messages').scrollHeight;
}
function connect(){
 ws=new WebSocket(`${wsProto}//${location.host}/ws/${room}`);
 ws.onopen=()=>{$('chatstatus').textContent='в сети';send({type:'hello'})};
 ws.onclose=()=>{$('chatstatus').textContent='переподключение…';setTimeout(connect,1500)};
 ws.onmessage=e=>{
  const d=JSON.parse(e.data);
  if(d.type==='count'){$('people').textContent=`${d.count}/2 в комнате`}
  if(d.type==='chat')addMessage(d.name,d.text);
  if(d.type==='system')addMessage('',d.text,true);
  if(d.type==='load' && d.url!==$('url').value)loadUrl(d.url,false);
  if(d.type==='control')applyControl(d);
 };
}
function youtubeId(url){
 try{const u=new URL(url);if(u.hostname.includes('youtu.be'))return u.pathname.slice(1);
 if(u.hostname.includes('youtube.com')){if(u.pathname==='/watch')return u.searchParams.get('v');if(u.pathname.startsWith('/embed/'))return u.pathname.split('/')[2];if(u.pathname.startsWith('/shorts/'))return u.pathname.split('/')[2]}}
 catch(e){} return null;
}
function loadUrl(url,broadcast=true){
 url=(url||'').trim();if(!url)return;
 try{new URL(url)}catch(e){addMessage('necar watch','Проверьте ссылку.',true);return}
 $('url').value=url;const holder=$('player');holder.innerHTML='';nativeVideo=null;ytPlayer=null;currentType='';
 const yt=youtubeId(url);
 if(yt){
  currentType='youtube';const frame=document.createElement('div');frame.id='ytplayer';holder.appendChild(frame);
  const s=document.createElement('script');s.src='https://www.youtube.com/iframe_api';document.head.appendChild(s);
  window.onYouTubeIframeAPIReady=()=>{ytPlayer=new YT.Player('ytplayer',{width:'100%',height:'100%',videoId:yt,playerVars:{playsinline:1,rel:0},events:{onReady:()=>{}}})};
 } else if(/\.(mp4|webm|ogg|m4v|mov)(\?.*)?$/i.test(new URL(url).pathname+new URL(url).search)){
  currentType='video';nativeVideo=document.createElement('video');nativeVideo.src=url;nativeVideo.controls=true;nativeVideo.playsInline=true;nativeVideo.addEventListener('play',()=>{if(!lastRemote)send({type:'control',action:'play',time:nativeVideo.currentTime})});nativeVideo.addEventListener('pause',()=>{if(!lastRemote)send({type:'control',action:'pause',time:nativeVideo.currentTime})});nativeVideo.addEventListener('seeked',()=>{if(!lastRemote)send({type:'control',action:'seek',time:nativeVideo.currentTime})});holder.appendChild(nativeVideo);
 } else {
  currentType='iframe';const frame=document.createElement('iframe');frame.src=url;frame.allow='autoplay; fullscreen; picture-in-picture';frame.allowFullscreen=true;frame.referrerPolicy='no-referrer-when-downgrade';holder.appendChild(frame);
 }
 if(broadcast)send({type:'load',url});
}
function applyControl(d){
 lastRemote=true;
 try{
  if(currentType==='video'&&nativeVideo){if(Number.isFinite(d.time))nativeVideo.currentTime=d.time;if(d.action==='play')nativeVideo.play().catch(()=>{});if(d.action==='pause')nativeVideo.pause()}
  else if(currentType==='youtube'&&ytPlayer&&ytPlayer.seekTo){if(Number.isFinite(d.time))ytPlayer.seekTo(d.time,true);if(d.action==='play')ytPlayer.playVideo();if(d.action==='pause')ytPlayer.pauseVideo()}
 }catch(e){}
 setTimeout(()=>lastRemote=false,350);
}
$('load').onclick=()=>loadUrl($('url').value);
$('url').addEventListener('keydown',e=>{if(e.key==='Enter')loadUrl($('url').value)});
$('copy').onclick=async()=>{try{await navigator.clipboard.writeText(location.href);$('copy').textContent='Скопировано!';setTimeout(()=>$('copy').textContent='Копировать приглашение',1500)}catch(e){prompt('Скопируйте ссылку:',location.href)}};
$('chatform').addEventListener('submit',e=>{e.preventDefault();const text=$('message').value.trim();if(text){send({type:'chat',text});$('message').value=''}});
$('play').onclick=()=>{if(currentType==='video'&&nativeVideo){if(nativeVideo.paused)nativeVideo.play().catch(()=>{});else nativeVideo.pause();send({type:'control',action:nativeVideo.paused?'play':'pause',time:nativeVideo.currentTime})}else if(currentType==='youtube'&&ytPlayer){const state=ytPlayer.getPlayerState();if(state===1){ytPlayer.pauseVideo();send({type:'control',action:'pause',time:ytPlayer.getCurrentTime()})}else{ytPlayer.playVideo();send({type:'control',action:'play',time:ytPlayer.getCurrentTime()})}}else addMessage('necar watch','Для этого сайта кнопки управления могут не работать — используйте плеер самого сайта.',true)};
$('back').onclick=()=>seekBy(-10);$('forward').onclick=()=>seekBy(10);
function seekBy(delta){if(currentType==='video'&&nativeVideo){nativeVideo.currentTime=Math.max(0,nativeVideo.currentTime+delta);send({type:'control',action:'seek',time:nativeVideo.currentTime})}else if(currentType==='youtube'&&ytPlayer){const t=Math.max(0,ytPlayer.getCurrentTime()+delta);ytPlayer.seekTo(t,true);send({type:'control',action:'seek',time:t})}}
connect();
</script>
</body></html>"""

@app.get("/")
async def home():
    room = secrets.token_urlsafe(5)
    from fastapi.responses import RedirectResponse
    return RedirectResponse(f"/room/{room}")

@app.get("/room/{room}")
async def room_page(room: str):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", room):
        return HTMLResponse("Некорректная комната", status_code=400)
    return HTMLResponse(HTML)

@app.websocket("/ws/{room}")
async def websocket_endpoint(websocket: WebSocket, room: str):
    await websocket.accept()
    peers = rooms[room]
    if len(peers) >= MAX_PEOPLE:
        await websocket.send_text(json.dumps({"type":"system","text":"Комната уже заполнена (максимум 2 человека)."}))
        await websocket.close(code=1008)
        return
    peers.add(websocket)
    async def broadcast(payload, exclude=None):
        raw = json.dumps(payload, ensure_ascii=False)
        for peer in list(peers):
            if peer is not exclude:
                try: await peer.send_text(raw)
                except Exception: peers.discard(peer)
    await broadcast({"type":"count","count":len(peers)})
    await broadcast({"type":"system","text":"Партнёр подключился." if len(peers)==2 else "Вы в комнате. Ожидаем второго участника."})
    try:
        while True:
            data = json.loads(await websocket.receive_text())
            kind = data.get("type")
            if kind == "hello":
                if room in room_state:
                    await websocket.send_text(json.dumps(room_state[room], ensure_ascii=False))
            elif kind == "chat":
                text = str(data.get("text",""))[:1000].strip()
                if text: await broadcast({"type":"chat","name":"Участник","text":text})
            elif kind == "load":
                url = str(data.get("url",""))[:2000]
                if url.startswith(("https://","http://")):
                    room_state[room] = {"type":"load","url":url}
                    await broadcast({"type":"load","url":url}, exclude=websocket)
            elif kind == "control":
                action = data.get("action")
                if action in ("play","pause","seek"):
                    try: t = max(0, float(data.get("time",0)))
                    except (ValueError, TypeError): t = 0
                    await broadcast({"type":"control","action":action,"time":t}, exclude=websocket)
    except (WebSocketDisconnect, json.JSONDecodeError):
        pass
    finally:
        peers.discard(websocket)
        await broadcast({"type":"count","count":len(peers)})
        if not peers:
            rooms.pop(room, None)
            room_state.pop(room, None)
