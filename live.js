/**
 * Shadow Nexus Social — live.js
 *
 * Main orchestrator for the standalone live.html page.
 *
 * Responsibilities:
 *  1. Auth — resolve current user from Firebase Auth
 *  2. Camera / Microphone — getUserMedia, preview toggles
 *  3. Lobby — load active rooms, render room cards
 *  4. Host flow — create room → start stream → accept viewers
 *  5. Viewer flow — join room → WebRTC connect → watch
 *  6. Comments — send & receive in real time
 *  7. Likes — send like, display floating hearts
 *  8. End / Leave — cleanup connections, return to lobby
 *  9. Navigation — back button / URL param ?watch=<roomId>
 */

import { LiveDB }           from './firebase-live.js';
import { hostAcceptViewer, viewerConnect } from './webrtc.js';

import { getApps, getApp, initializeApp }
  from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js';
import { getAuth, onAuthStateChanged }
  from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-auth.js';

/* ── Bootstrap Firebase Auth (reuse existing app or init a new one) ── */
const _AUTH_CFG = {
  apiKey:            'AIzaSyByZRmp6R9HY17T2_WdJUFWeeaLNOP6y2Y',
  authDomain:        'horr-a08f4.firebaseapp.com',
  databaseURL:       'https://horr-a08f4-default-rtdb.firebaseio.com',
  projectId:         'horr-a08f4',
  storageBucket:     'horr-a08f4.firebasestorage.app',
  messagingSenderId: '933810617818',
  appId:             '1:933810617818:web:efb24f123337dd987c14e3',
};
const _authApp  = getApps().length
  ? (getApps().find(a => a.name === 'snx-live') || getApp())
  : initializeApp(_AUTH_CFG);
const _auth     = getAuth(_authApp);

/* ── Cloudflare R2 worker URL (for thumbnail uploads) ── */
const R2_URL = 'https://yellow-term-11e6.nthntjrn.workers.dev';

/* ════════════════════════════════════════
   STATE
   ════════════════════════════════════════ */
let _user        = null;   // Firebase User
let _roomId      = null;   // active room ID
let _isHost      = false;
let _stream      = null;   // local MediaStream
let _pc          = null;   // RTCPeerConnection (viewer side)
let _previewMuted  = false;
let _previewCamOff = false;
let _micMuted    = false;
let _camOff      = false;
let _unsubs      = [];     // RTDB ref handles to clean up

/* ════════════════════════════════════════
   DOM HELPERS
   ════════════════════════════════════════ */
const $ = id => document.getElementById(id);

function showView(name) {
  $('lobbyView').style.display   = name === 'lobby'   ? 'flex'  : 'none';
  $('previewView').style.display = name === 'preview' ? 'flex'  : 'none';
  $('roomView').style.display    = name === 'room'    ? 'block' : 'none';
}

function esc(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function toast(msg) {
  /* If loaded inside the main app, reuse its toast; otherwise alert */
  if (typeof window.toastNotification === 'function') {
    window.toastNotification(msg);
  } else {
    const d = document.createElement('div');
    d.style.cssText = 'position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:rgba(8,20,48,0.97);border:1px solid rgba(0,174,239,0.55);padding:10px 20px;border-radius:10px;color:#c8e8ff;font-size:13px;z-index:99999;max-width:90vw;text-align:center;';
    d.textContent = msg;
    document.body.appendChild(d);
    setTimeout(() => d.remove(), 3000);
  }
}

/* ════════════════════════════════════════
   AUTH INIT
   ════════════════════════════════════════ */
onAuthStateChanged(_auth, user => {
  _user = user;
  /* Disable Go Live button if not signed in */
  const btn = $('goLiveBtn');
  if (btn) btn.disabled = !user;
});

/* ════════════════════════════════════════
   NAVIGATION
   ════════════════════════════════════════ */

/** Navigate back to the main app (or just close the tab) */
window.goBack = function() {
  if (document.referrer && document.referrer.includes(location.hostname)) {
    history.back();
  } else {
    location.href = 'index.html';
  }
};

/* ── URL param: ?watch=<roomId> auto-joins a room ── */
(function checkUrlParams() {
  const params = new URLSearchParams(location.search);
  const watchId = params.get('watch');
  if (watchId) {
    /* Wait until auth resolves before joining */
    const wait = setInterval(() => {
      clearInterval(wait);
      joinRoom(watchId);
    }, 600);
  } else {
    refreshRooms();
  }
})();

/* ════════════════════════════════════════
   LOBBY
   ════════════════════════════════════════ */
window.refreshRooms = async function() {
  const list = $('roomList');
  list.innerHTML = '<div class="empty-state"><div class="empty-icon">⏳</div>Loading…</div>';

  const rooms = await LiveDB.getActiveRooms();
  if (!rooms.length) {
    list.innerHTML = '<div class="empty-state"><div class="empty-icon">📡</div>No one is live right now.<br>Be the first to go live!</div>';
    return;
  }

  list.innerHTML = '';
  for (const room of rooms) {
    const initial    = (room.host?.username || '?').charAt(0).toUpperCase();
    const imgSrc     = room.host?.thumbnail || room.host?.avatar || '';
    const avatarStyle = imgSrc
      ? `background-image:url('${esc(imgSrc)}');background-size:cover;background-position:center;`
      : '';
    const viewers    = room.viewerCount || 0;
    const likes      = room.likes?.count || 0;

    const card = document.createElement('div');
    card.className = 'room-card';
    card.innerHTML = `
      <div class="room-avatar" style="${avatarStyle}">${avatarStyle ? '' : initial}</div>
      <div class="room-info">
        <div class="room-host">${esc(room.host?.username || 'Unknown')}</div>
        <div class="room-meta">👁 ${viewers} watching · ❤️ ${likes}</div>
      </div>
      <button class="watch-btn" data-rid="${esc(room.id)}">▶ Watch</button>
    `;
    card.querySelector('.watch-btn').addEventListener('click', () => joinRoom(room.id));
    list.appendChild(card);
  }
};

/* ════════════════════════════════════════
   CAMERA PREVIEW (pre-LIVE)
   ════════════════════════════════════════ */
window.openPreview = async function() {
  if (!_user) { toast('Sign in to go live.'); return; }

  showView('preview');
  $('startLiveBtn').disabled = true;

  try {
    _stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
    $('previewVideo').srcObject = _stream;
    $('camError').style.display = 'none';
    $('startLiveBtn').disabled  = false;
  } catch (err) {
    console.warn('[LIVE] getUserMedia failed', err);
    $('camError').style.display = 'block';
  }
};

window.cancelPreview = function() {
  _stopStream();
  $('previewVideo').srcObject = null;
  showView('lobby');
};

window.togglePreviewMic = function() {
  if (!_stream) return;
  _previewMuted = !_previewMuted;
  _stream.getAudioTracks().forEach(t => { t.enabled = !_previewMuted; });
  const btn = $('muteBtn');
  btn.textContent = _previewMuted ? '🔇' : '🎙️';
  btn.classList.toggle('muted', _previewMuted);
};

window.togglePreviewCam = function() {
  if (!_stream) return;
  _previewCamOff = !_previewCamOff;
  _stream.getVideoTracks().forEach(t => { t.enabled = !_previewCamOff; });
  const btn = $('camOffBtn');
  btn.textContent = _previewCamOff ? '📵' : '📷';
  btn.classList.toggle('muted', _previewCamOff);
};

/* ════════════════════════════════════════
   HOST: START LIVE
   ════════════════════════════════════════ */
window.startLive = async function() {
  if (!_user || !_stream) return;

  const btn = $('startLiveBtn');
  btn.disabled   = true;
  btn.textContent = '⏳ Starting…';

  try {
    /* 1. Create room in Firebase RTDB */
    _roomId  = await LiveDB.createRoom({
      uid:      _user.uid,
      username: _user.displayName || 'Anonymous',
      avatar:   _user.photoURL    || '',
    });
    _isHost = true;

    /* 2. Enter full-screen live room */
    _enterRoom(true);

    /* 3. Attach local stream to host video */
    const hv = $('hostVideo');
    hv.srcObject     = _stream;
    hv.style.display = 'block';
    $('remoteVideo').style.display = 'none';

    /* 4. Show host controls */
    $('hostControls').style.display = 'flex';
    $('endBtn').style.display       = 'inline-flex';
    $('leaveBtn').style.display     = 'none';
    $('hostNameEl').textContent     = _user.displayName || 'Me';

    /* 5. Listen for viewer offers (WebRTC) */
    _hostListenForOffers();

    /* 6. Subscribe realtime data */
    _subscribeComments();
    _subscribeLikes();
    _subscribeViewers();

    /* 7. Upload thumbnail to R2 after the stream starts */
    _captureAndUploadThumbnail();

  } catch (err) {
    console.error('[LIVE] startLive error', err);
    toast('Could not start live. Please try again.');
    btn.disabled   = false;
    btn.textContent = '🔴 Start LIVE';
  }
};

/* ── Capture a JPEG frame from the preview video and upload to R2 ── */
async function _captureAndUploadThumbnail() {
  try {
    const vid = $('previewVideo');
    if (!vid || !_roomId) return;

    const blob = await new Promise(resolve => {
      try {
        const W = 640, H = 360;
        const cvs = document.createElement('canvas');
        cvs.width = W; cvs.height = H;
        const ctx = cvs.getContext('2d');
        ctx.translate(W, 0); ctx.scale(-1, 1);      // mirror to match preview
        ctx.drawImage(vid, 0, 0, W, H);
        cvs.toBlob(resolve, 'image/jpeg', 0.75);
      } catch (_) { resolve(null); }
    });
    if (!blob) return;

    const fd = new FormData();
    fd.append('file', new File([blob], 'live-thumb.jpg', { type: 'image/jpeg' }));
    fd.append('uid',  _user.uid);
    const res  = await fetch(R2_URL, { method: 'POST', body: fd });
    const json = await res.json();
    if (json?.url) await LiveDB.setThumbnail(_roomId, json.url);
  } catch (e) {
    console.warn('[LIVE] thumbnail upload failed', e);
  }
}

/* ── Host: listen for viewer WebRTC offers ── */
function _hostListenForOffers() {
  const r = LiveDB.subscribeOffers(_roomId, async (viewerKey, offer) => {
    const pc = await hostAcceptViewer({
      stream:    _stream,
      roomId:    _roomId,
      viewerKey,
      offer,
      db:        LiveDB,
    });
    /* Track PCs for cleanup (basic: keep last one) */
    _pc = pc;
  });
  _unsubs.push(r);
}

/* ════════════════════════════════════════
   VIEWER: JOIN ROOM
   ════════════════════════════════════════ */
window.joinRoom = async function(roomId) {
  if (!_user) { toast('Sign in to watch live streams.'); return; }

  /* Verify room is still live */
  const host = await LiveDB.getRoomHost(roomId);
  if (!host?.isLive) { toast('This stream has ended.'); refreshRooms(); return; }

  _roomId = roomId;
  _isHost = false;

  await LiveDB.viewerJoin(roomId, _user.uid, _user.displayName || 'Anonymous');

  /* Enter UI */
  _enterRoom(false);
  $('endBtn').style.display       = 'none';
  $('leaveBtn').style.display     = 'inline-flex';
  $('hostControls').style.display = 'none';
  $('hostNameEl').textContent     = host.username || 'Host';

  /* Realtime data */
  _subscribeComments();
  _subscribeLikes();
  _subscribeViewers();

  /* WebRTC: request host stream */
  _pc = await viewerConnect({
    roomId,
    viewerUid: _user.uid,
    db:        LiveDB,
    onStream:  stream => {
      const rv = $('remoteVideo');
      rv.srcObject     = stream;
      rv.style.display = 'block';
      $('hostVideo').style.display = 'none';
    },
  });

  /* Fallback: if no stream in 10s, show message */
  setTimeout(() => {
    if (!$('remoteVideo').srcObject && _roomId === roomId) {
      _appendComment({ username: '📡 System', message: 'Connecting to stream…' });
    }
  }, 10000);
};

/* ════════════════════════════════════════
   ENTER LIVE ROOM UI
   ════════════════════════════════════════ */
function _enterRoom(isHost) {
  showView('room');
  $('commentsArea').innerHTML    = '';
  $('commentInput').value        = '';
  $('viewerCountEl').textContent = '0';
  $('likeCountEl').textContent   = '0';
}

/* ════════════════════════════════════════
   COMMENTS
   ════════════════════════════════════════ */
function _subscribeComments() {
  const r = LiveDB.subscribeComments(_roomId, msgs => {
    const area = $('commentsArea');
    if (!area) return;
    area.innerHTML = '';
    msgs.forEach(m => _appendComment(m));
    area.scrollTop = area.scrollHeight;
  });
  _unsubs.push(r);
}

function _appendComment({ username, message }) {
  const area = $('commentsArea');
  if (!area) return;
  const item = document.createElement('div');
  item.className = 'comment-item';
  item.innerHTML = `
    <span class="comment-author">${esc(username || 'Anonymous')}:</span>
    <span class="comment-text">${esc(message || '')}</span>
  `;
  area.appendChild(item);
  area.scrollTop = area.scrollHeight;
}

window.sendComment = async function() {
  const input = $('commentInput');
  const msg   = input?.value?.trim();
  if (!msg || !_roomId || !_user) return;
  input.value = '';
  try {
    await LiveDB.pushComment(_roomId, _user.displayName || 'Anonymous', msg);
  } catch (e) {
    console.warn('[LIVE] sendComment error', e);
  }
};

/* ════════════════════════════════════════
   LIKES
   ════════════════════════════════════════ */
function _subscribeLikes() {
  const r = LiveDB.subscribeLikes(_roomId, count => {
    const el = $('likeCountEl');
    if (el) el.textContent = count;
  });
  _unsubs.push(r);
}

window.doLike = async function() {
  if (!_roomId || !_user) return;
  _floatHeart();
  try { await LiveDB.incrementLikes(_roomId); }
  catch (e) { console.warn('[LIVE] doLike error', e); }
};

function _floatHeart() {
  const room = $('roomView');
  if (!room) return;
  const h = document.createElement('div');
  h.className  = 'float-heart';
  h.textContent = '❤️';
  h.style.right  = (10 + Math.random() * 30) + 'px';
  h.style.bottom = (80 + Math.random() * 30) + 'px';
  room.appendChild(h);
  setTimeout(() => h.remove(), 2100);
}

/* ════════════════════════════════════════
   VIEWER COUNT
   ════════════════════════════════════════ */
function _subscribeViewers() {
  const r = LiveDB.subscribeViewers(_roomId, count => {
    const el = $('viewerCountEl');
    if (el) el.textContent = count;
  });
  _unsubs.push(r);
}

/* ════════════════════════════════════════
   HOST CONTROLS (mute / camera)
   ════════════════════════════════════════ */
window.toggleMic = function() {
  if (!_stream) return;
  _micMuted = !_micMuted;
  _stream.getAudioTracks().forEach(t => { t.enabled = !_micMuted; });
  const btn = $('liveMuteBtn');
  btn.textContent = _micMuted ? '🔇' : '🎙️';
  btn.classList.toggle('muted', _micMuted);
};

window.toggleCam = function() {
  if (!_stream) return;
  _camOff = !_camOff;
  _stream.getVideoTracks().forEach(t => { t.enabled = !_camOff; });
  const btn = $('liveCamBtn');
  btn.textContent = _camOff ? '📵' : '📷';
  btn.classList.toggle('cam-off', _camOff);
};

/* ════════════════════════════════════════
   END / LEAVE
   ════════════════════════════════════════ */
window.endStream = async function() {
  if (!_isHost || !_roomId) return;
  try { await LiveDB.endRoom(_roomId); } catch (_) {}
  _cleanup();
  showView('lobby');
  refreshRooms();
  toast('Your live stream has ended.');
};

window.leaveStream = async function() {
  if (!_roomId || !_user) { showView('lobby'); return; }
  try { await LiveDB.viewerLeave(_roomId, _user.uid); } catch (_) {}
  _cleanup();
  showView('lobby');
  refreshRooms();
};

function _cleanup() {
  /* Unsubscribe all RTDB listeners */
  for (const r of _unsubs) LiveDB.unsub(r);
  _unsubs = [];

  /* Close WebRTC peer connection */
  if (_pc) { try { _pc.close(); } catch (_) {} _pc = null; }

  /* Stop local camera / mic */
  _stopStream();

  /* Reset video elements */
  const hv = $('hostVideo'),  rv = $('remoteVideo');
  if (hv) hv.srcObject = null;
  if (rv) { rv.srcObject = null; rv.style.display = 'none'; }

  _roomId = null;
  _isHost = false;
  _micMuted = false;
  _camOff   = false;
}

function _stopStream() {
  if (_stream) { _stream.getTracks().forEach(t => t.stop()); _stream = null; }
}

/* ════════════════════════════════════════
   CLEANUP ON UNLOAD / BACK
   ════════════════════════════════════════ */
window.addEventListener('beforeunload', () => {
  if (_roomId && _isHost) {
    LiveDB.endRoom(_roomId).catch(() => {});
  } else if (_roomId && _user) {
    LiveDB.viewerLeave(_roomId, _user.uid).catch(() => {});
  }
});

window.addEventListener('popstate', () => {
  if (_roomId) {
    if (_isHost) endStream();
    else         leaveStream();
  }
});
