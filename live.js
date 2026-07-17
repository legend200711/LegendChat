/**
 * live.js — Shadow Nexus Social · 8-Box WebRTC Live Streaming Engine
 *
 * Architecture
 * ─────────────────────────────────────────────────────────────────
 *  • Firebase Firestore — signaling (offer/answer/ICE) + room metadata
 *  • Firebase Realtime Database — viewer count, chat, reactions
 *  • WebRTC PeerConnection per guest (fully isolated — one crash ≠ all crash)
 *  • Adaptive bitrate: degrades 720p → 480p → 360p → audio-only based on RTT / loss
 *  • Auto-reconnect on ICE failure (exponential back-off, max 8 retries)
 *  • Per-box reconnect overlay + connection status indicator
 *  • Per-box controls: refresh, mute, cam-restart, remove (host-only remove)
 *  • Active-speaker detection via Web Audio API (VAD)
 *  • Host controls: mute, cam-off, remove, lock room, restart guest connection
 *  • Mobile: camera rotation, battery/perf throttle, active-speaker mode
 *  • Network speed detection (navigator.connection + speed probe)
 *  • WiFi ↔ 5G/4G network-switch handler — no reconnect, just quality recheck
 *  • Audio-priority mode: voice preserved when video must be sacrificed
 *  • Guest self-recovery: guest can re-initiate own peer on disconnect
 *  • Camera/mic error overlays — never a blank black box
 */

import { initializeApp, getApps }
  from "https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js";
import { getAuth, onAuthStateChanged }
  from "https://www.gstatic.com/firebasejs/10.8.0/firebase-auth.js";
import {
  getFirestore, doc, setDoc, getDoc, updateDoc, deleteDoc,
  collection, addDoc, onSnapshot, serverTimestamp, query, orderBy, limit
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-firestore.js";
import {
  getDatabase, ref, set, push, onValue, off, remove, increment as rtIncrement, onDisconnect
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";

// ─────────────────────────────────────────────────────────────────
// Firebase init (re-use existing app if already initialised)
// ─────────────────────────────────────────────────────────────────
const FB_CONFIG = {
  apiKey:            "AIzaSyByZRmp6R9HY17T2_WdJUFWeeaLNOP6y2Y",
  authDomain:        "horr-a08f4.firebaseapp.com",
  databaseURL:       "https://horr-a08f4-default-rtdb.firebaseio.com",
  projectId:         "horr-a08f4",
  storageBucket:     "horr-a08f4.firebasestorage.app",
  messagingSenderId: "933810617818",
  appId:             "1:933810617818:web:efb24f123337dd987c14e3"
};
const fbApp  = getApps().length ? getApps()[0] : initializeApp(FB_CONFIG);
const auth   = getAuth(fbApp);
const db     = getFirestore(fbApp);
const rtdb   = getDatabase(fbApp);

// ─────────────────────────────────────────────────────────────────
// ICE servers (STUN + fallback TURN)
// ─────────────────────────────────────────────────────────────────
const ICE_SERVERS = [
  { urls: "stun:stun.l.google.com:19302"   },
  { urls: "stun:stun1.l.google.com:19302"  },
  { urls: "stun:stun2.l.google.com:19302"  }
  // Add TURN credentials here when available:
  // { urls: "turn:your-turn-server:3478", username: "user", credential: "pass" }
];

// ─────────────────────────────────────────────────────────────────
// Quality presets — applied to the local sender encodings
// VERY_LOW = audio-priority: video at minimum, audio preserved
// ─────────────────────────────────────────────────────────────────
const QUALITY = {
  HIGH:      { width: 1280, height: 720,  frameRate: 30, bitrate: 1_500_000, audioBitrate: 64_000 },
  MEDIUM:    { width: 854,  height: 480,  frameRate: 24, bitrate:   700_000, audioBitrate: 48_000 },
  LOW:       { width: 640,  height: 360,  frameRate: 15, bitrate:   300_000, audioBitrate: 32_000 },
  VERY_LOW:  { width: 320,  height: 240,  frameRate: 10, bitrate:   100_000, audioBitrate: 24_000 },
};
const QUALITY_THRESHOLDS = {
  rttHigh:        250,   // ms  → drop to MEDIUM
  rttCritical:    450,   // ms  → drop to LOW
  rttExtreme:     800,   // ms  → drop to VERY_LOW (audio priority)
  lossHigh:       0.05,  // 5%  → drop to MEDIUM
  lossCritical:   0.12,  // 12% → drop to LOW
  lossExtreme:    0.25,  // 25% → drop to VERY_LOW
};

// Network tier → quality cap (so a weak mobile signal never wastes bandwidth)
const NETWORK_QUALITY_CAP = {
  "4g":         "HIGH",
  "3g":         "MEDIUM",
  "2g":         "LOW",
  "slow-2g":    "VERY_LOW",
  "wifi":       "HIGH",     // non-standard but some browsers report it
  "ethernet":   "HIGH",
  "bluetooth":  "LOW",
  unknown:      "HIGH",
};

// ─────────────────────────────────────────────────────────────────
// Runtime state
// ─────────────────────────────────────────────────────────────────
let currentUser     = null;
let myDisplayName   = "Guest";
let roomId          = null;
let isHost          = false;
let roomLocked      = false;
let liveActive      = false;
let localStream     = null;
let micEnabled      = true;
let camEnabled      = true;
let facingMode      = "user";
let currentQuality  = "HIGH";
let _networkTier    = "HIGH";   // derived from navigator.connection
let _connMonInterval = null;    // heartbeat for self-recovery (guest)

// guests[uid] = { pc, stream, displayName, muted, camOff, retries, quality, _qualityInterval }
const guests = {};

// Firestore unsub handles
const _unsubs = [];

// RTDB refs
let roomRtRef   = null;
let viewerRef   = null;
let chatRtRef   = null;
let presenceRef = null;

// ─────────────────────────────────────────────────────────────────
// Network speed / tier detection
// ─────────────────────────────────────────────────────────────────
function detectNetworkTier() {
  const conn = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
  if (!conn) return "HIGH";
  const ect = conn.effectiveType || "unknown";
  return NETWORK_QUALITY_CAP[ect] || NETWORK_QUALITY_CAP[conn.type] || "HIGH";
}

function capQualityByNetwork(target) {
  const order = ["VERY_LOW", "LOW", "MEDIUM", "HIGH"];
  const capIdx = order.indexOf(_networkTier);
  const tgtIdx = order.indexOf(target);
  return tgtIdx > capIdx ? _networkTier : target;
}

function initNetworkListener() {
  const conn = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
  if (!conn) return;
  conn.addEventListener("change", () => {
    const prev = _networkTier;
    _networkTier = detectNetworkTier();
    if (prev !== _networkTier) {
      toast(`Network changed → ${_networkTier === "HIGH" ? "Fast" : _networkTier === "MEDIUM" ? "Average" : _networkTier === "LOW" ? "Slow" : "Very Slow"} connection`);
      // Re-evaluate quality for all peers without triggering a full reconnect
      Object.keys(guests).forEach(uid => {
        const g = guests[uid];
        if (g && g.quality) {
          const capped = capQualityByNetwork(g.quality);
          if (capped !== g.quality) {
            g.quality = capped;
            applyQualityToSender(g.pc, capped).catch(() => {});
            updateQualityDot(uid, capped);
          }
        }
      });
    }
  });
}

// DOM shorthand
const $  = id => document.getElementById(id);
const el = (tag, cls, inner) => {
  const e = document.createElement(tag);
  if (cls)   e.className = cls;
  if (inner !== undefined) e.innerHTML = inner;
  return e;
};

// ─────────────────────────────────────────────────────────────────
// Entry point — wait for auth
// ─────────────────────────────────────────────────────────────────
onAuthStateChanged(auth, async user => {
  if (!user) {
    window.location.href = "index.html";
    return;
  }
  currentUser = user;

  try {
    const snap = await getDoc(doc(db, "users", user.uid));
    if (snap.exists()) myDisplayName = snap.data().displayName || snap.data().username || "User";
  } catch (_) { /* best-effort */ }

  initUI();
});

// ─────────────────────────────────────────────────────────────────
// UI init
// ─────────────────────────────────────────────────────────────────
function initUI() {
  _networkTier = detectNetworkTier();
  initNetworkListener();
  buildVideoGrid();
  attachButtonHandlers();
  attachKeyboardHandlers();
  showLobby();

  const params = new URLSearchParams(location.search);
  if (params.has("room")) {
    // roomId can be a Firestore doc id (long) or a legacy 6-char code
    const code = params.get("room").trim().slice(0, 60);
    if (params.get("host") === "1") {
      startAsHost(code);
    } else {
      // Guest arriving from the Feed — auto-request to join, no code screen shown
      requestToJoin(code);
    }
  }
}

// ─────────────────────────────────────────────────────────────────
// Build 8 video boxes in the grid
// ─────────────────────────────────────────────────────────────────
function buildVideoGrid() {
  const grid = $("video-grid");
  grid.innerHTML = "";
  for (let i = 0; i < 8; i++) {
    const box = el("div", `video-box empty-slot${i === 0 ? " host-box" : ""}`, "");
    box.dataset.slot  = i;
    box.dataset.uid   = "";

    // Video element
    const vid = document.createElement("video");
    vid.autoplay    = true;
    vid.playsInline = true;
    vid.muted       = (i === 0); // mute local playback to avoid echo
    box.appendChild(vid);

    // Cam-off placeholder
    const ph = el("div", "cam-placeholder",
      `<div class="cam-placeholder-avatar">🌑</div><div class="cam-placeholder-name">—</div>`);
    box.appendChild(ph);

    // Error overlay — shown when cam/mic fails or connection is totally lost
    const errOv = el("div", "box-error-overlay",
      `<div class="box-error-icon">⚠️</div><div class="box-error-msg">No video</div>`);
    box.appendChild(errOv);

    // Reconnecting overlay — per-box spinner shown during ICE restart
    const reconOv = el("div", "box-reconnect-overlay",
      `<div class="box-recon-spinner"></div><div class="box-recon-msg">Reconnecting…</div>`);
    box.appendChild(reconOv);

    // Overlay (name / badges)
    const ov = el("div", "box-overlay",
      `<div class="box-badges"></div>
       <div class="box-host-tag" style="display:none;">HOST</div>
       <div class="box-name">Empty</div>`
    );
    box.appendChild(ov);

    // Connection status indicator
    const statusBar = el("div", "box-status-bar",
      `<span class="box-conn-dot good"></span><span class="box-conn-label">Good</span>`);
    box.appendChild(statusBar);

    // Per-box control buttons
    const boxCtrls = el("div", "box-controls", "");
    const btnRefresh = el("button", "box-ctrl-btn", "🔄");
    btnRefresh.title = "Refresh box";
    btnRefresh.addEventListener("click", e => { e.stopPropagation(); refreshBox(box.dataset.uid, box); });

    const btnBoxMute = el("button", "box-ctrl-btn", "🎤");
    btnBoxMute.title = "Mute/Unmute";
    btnBoxMute.addEventListener("click", e => { e.stopPropagation(); toggleBoxMute(box.dataset.uid, btnBoxMute); });

    const btnBoxCam = el("button", "box-ctrl-btn", "📷");
    btnBoxCam.title = "Restart camera";
    btnBoxCam.addEventListener("click", e => { e.stopPropagation(); restartBoxCam(box.dataset.uid); });

    const btnBoxRemove = el("button", "box-ctrl-btn box-ctrl-remove", "❌");
    btnBoxRemove.title = "Remove guest (host only)";
    btnBoxRemove.addEventListener("click", e => { e.stopPropagation(); removeBoxGuest(box.dataset.uid); });

    boxCtrls.appendChild(btnRefresh);
    boxCtrls.appendChild(btnBoxMute);
    boxCtrls.appendChild(btnBoxCam);
    boxCtrls.appendChild(btnBoxRemove);
    box.appendChild(boxCtrls);

    // Quality dot (legacy, kept for compat)
    const qd = el("div", "quality-dot good");
    box.appendChild(qd);

    // Empty label
    const lbl = el("div", "empty-slot-label", "Empty slot");
    box.appendChild(lbl);

    // Long-press / right-click for host context menu
    addContextMenuTrigger(box);

    grid.appendChild(box);
  }

  // Re-inject ctrl-bar into grid
  grid.appendChild($("ctrl-bar"));
}

// ─────────────────────────────────────────────────────────────────
// Slot helpers
// ─────────────────────────────────────────────────────────────────
function slotFor(uid) {
  return document.querySelector(`.video-box[data-uid="${uid}"]`);
}
function nextEmptySlot() {
  return document.querySelector('.video-box[data-uid=""]');
}
function assignSlot(uid, displayName, stream, isHostSlot) {
  const slot = isHostSlot
    ? document.querySelector('.video-box.host-box')
    : nextEmptySlot();
  if (!slot) return null;

  slot.dataset.uid = uid;
  slot.classList.remove("empty-slot", "box-error");

  const vid  = slot.querySelector("video");
  const name = slot.querySelector(".box-name");
  const tag  = slot.querySelector(".box-host-tag");
  const lbl  = slot.querySelector(".empty-slot-label");

  if (stream) { vid.srcObject = stream; vid.play().catch(() => {}); }
  name.textContent = displayName;
  if (isHostSlot) tag.style.display = "block";
  if (lbl) lbl.style.display = "none";

  slot.querySelector(".cam-placeholder-name").textContent = displayName;
  setBoxStatus(slot, "good");
  return slot;
}

function clearSlot(uid) {
  const slot = slotFor(uid);
  if (!slot) return;
  const vid  = slot.querySelector("video");
  vid.srcObject = null;
  slot.dataset.uid = "";
  slot.classList.add("empty-slot");
  slot.classList.remove("cam-off", "speaking", "box-reconnecting", "box-error");
  slot.querySelector(".box-name").textContent = "Empty";
  slot.querySelector(".box-host-tag").style.display = "none";
  slot.querySelector(".empty-slot-label").style.display = "";
  slot.querySelector(".cam-placeholder-name").textContent = "—";
  setBoxStatus(slot, "good");
}

// ─────────────────────────────────────────────────────────────────
// Per-box status helpers
// ─────────────────────────────────────────────────────────────────
// status: "good" | "weak" | "reconnecting" | "error"
function setBoxStatus(slotOrUid, status, errorMsg) {
  const slot = typeof slotOrUid === "string" ? slotFor(slotOrUid) : slotOrUid;
  if (!slot) return;

  const dot   = slot.querySelector(".box-conn-dot");
  const label = slot.querySelector(".box-conn-label");
  const errOv = slot.querySelector(".box-error-overlay");
  const reconOv = slot.querySelector(".box-reconnect-overlay");
  const errMsg  = slot.querySelector(".box-error-msg");

  // Reset classes
  if (dot) dot.className = `box-conn-dot ${status === "reconnecting" ? "reconnecting" : status === "weak" ? "weak" : status === "error" ? "error" : "good"}`;
  if (label) label.textContent = { good: "Good", weak: "Weak", reconnecting: "Reconnecting", error: "Error" }[status] || "Good";

  slot.classList.toggle("box-reconnecting", status === "reconnecting");
  slot.classList.toggle("box-error",        status === "error");

  if (reconOv) reconOv.classList.toggle("visible", status === "reconnecting");
  if (errOv)   errOv.classList.toggle("visible",   status === "error");
  if (errMsg && errorMsg) errMsg.textContent = errorMsg;
}

// ─────────────────────────────────────────────────────────────────
// Per-box control actions
// ─────────────────────────────────────────────────────────────────

// 🔄 Refresh a box — if it's your own box, re-acquire media; if guest (host), restart peer
function refreshBox(uid, boxEl) {
  if (!uid) return;
  if (uid === currentUser.uid) {
    // Re-acquire our own stream
    restartLocalStream();
  } else if (isHost) {
    // Host restarts peer for this guest
    reconnectPeer(uid);
    toast(`Restarting connection for ${guests[uid]?.displayName || uid}…`);
  }
}

// 🎤 Toggle mute on a box
function toggleBoxMute(uid, btn) {
  if (!uid) return;
  if (uid === currentUser.uid) {
    toggleMic();
  } else if (isHost) {
    hostMuteGuest(uid);
  }
}

// 📷 Restart camera for a box
function restartBoxCam(uid) {
  if (!uid) return;
  if (uid === currentUser.uid) {
    restartLocalCam();
  } else if (isHost) {
    hostDisableCam(uid);
    toast(`Toggling camera for ${guests[uid]?.displayName || uid}…`);
  }
}

// ❌ Remove a guest box (host only)
function removeBoxGuest(uid) {
  if (!uid || uid === currentUser.uid) return;
  if (!isHost) return;
  if (confirm(`Remove ${guests[uid]?.displayName || "this guest"} from the Live?`)) {
    hostRemoveGuest(uid);
  }
}

// Restart our own local stream (camera/mic recovery)
async function restartLocalStream() {
  const slot = slotFor(currentUser.uid);
  setBoxStatus(slot, "reconnecting");
  try {
    const old = localStream;
    old?.getTracks().forEach(t => t.stop());
    localStream = null;
    await acquireLocalStream();
    // Replace tracks in all peer connections
    const newVid = localStream.getVideoTracks()[0];
    const newAud = localStream.getAudioTracks()[0];
    Object.values(guests).forEach(g => {
      g.pc.getSenders().forEach(s => {
        if (s.track?.kind === "video" && newVid) s.replaceTrack(newVid).catch(() => {});
        if (s.track?.kind === "audio" && newAud) s.replaceTrack(newAud).catch(() => {});
      });
    });
    if (slot) {
      const vid = slot.querySelector("video");
      vid.srcObject = localStream;
      vid.play().catch(() => {});
    }
    setBoxStatus(slot, "good");
    toast("Camera restarted ✓");
  } catch (e) {
    setBoxStatus(slot, "error", "Camera unavailable.");
  }
}

// Restart camera only (keep audio)
async function restartLocalCam() {
  const slot = slotFor(currentUser.uid);
  setBoxStatus(slot, "reconnecting");
  try {
    const qual = QUALITY[currentQuality];
    const newStream = await navigator.mediaDevices.getUserMedia({
      video: { width: { ideal: qual.width }, height: { ideal: qual.height }, frameRate: { ideal: qual.frameRate }, facingMode },
      audio: false
    });
    const newVid = newStream.getVideoTracks()[0];
    // Stop old video track
    localStream?.getVideoTracks().forEach(t => t.stop());
    // Replace in localStream
    if (localStream) {
      localStream.getVideoTracks().forEach(t => localStream.removeTrack(t));
      localStream.addTrack(newVid);
    }
    // Replace in all peers
    Object.values(guests).forEach(g => {
      const s = g.pc.getSenders().find(s => s.track?.kind === "video");
      if (s) s.replaceTrack(newVid).catch(() => {});
    });
    if (slot) {
      const vid = slot.querySelector("video");
      vid.srcObject = localStream;
      vid.play().catch(() => {});
    }
    setBoxStatus(slot, "good");
    toast("Camera restarted ✓");
  } catch (e) {
    setBoxStatus(slot, "error", "Camera unavailable.");
  }
}

// ─────────────────────────────────────────────────────────────────
// Button handlers
// ─────────────────────────────────────────────────────────────────
function attachButtonHandlers() {
  $("btnBack").onclick          = handleBack;
  $("btnGoLive").onclick        = handleGoLive;
  $("btnEndLive").onclick       = handleEndLive;
  $("btnMic").onclick           = toggleMic;
  $("btnCam").onclick           = toggleCam;
  $("btnFlip").onclick          = flipCamera;
  $("btnLock").onclick          = toggleLock;
  $("btnHostRoom").onclick      = () => startAsHost();
  // btnJoinRoom removed — public discovery is via the Feed
  $("btnBackHome").onclick      = () => window.location.href = "index.html";
  $("btnJoinCancel").onclick    = () => { hideOverlay("join-overlay"); showLobby(); };
  $("btnCancelRequest").onclick = cancelJoinRequest;
  $("chat-send").onclick        = sendChat;
  $("chat-input").addEventListener("keydown", e => { if (e.key === "Enter") sendChat(); });

  // Dismiss context menu on outside click
  document.addEventListener("click", e => {
    if (!e.target.closest("#guest-ctx-menu")) hideCtxMenu();
  });
}

function attachKeyboardHandlers() {
  document.addEventListener("keydown", e => {
    if (!liveActive) return;
    if (e.altKey && e.key === "m") toggleMic();
    if (e.altKey && e.key === "v") toggleCam();
  });
}

// ─────────────────────────────────────────────────────────────────
// Overlay helpers
// ─────────────────────────────────────────────────────────────────
function showLobby()      { hideAll(); $("lobby-overlay").classList.remove("hidden"); }
function showJoinOverlay(){ hideAll(); $("join-overlay").classList.remove("hidden"); }
function hideOverlay(id)  { $(id).classList.add("hidden"); }
function hideAll()        { ["lobby-overlay","join-overlay","waiting-overlay"].forEach(hideOverlay); }

// ─────────────────────────────────────────────────────────────────
// Generate a random 6-char room code
// ─────────────────────────────────────────────────────────────────
function genRoomCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  return Array.from({length:6}, () => chars[Math.floor(Math.random()*chars.length)]).join("");
}

// ─────────────────────────────────────────────────────────────────
// Host flow
// ─────────────────────────────────────────────────────────────────
async function startAsHost(code) {
  isHost = true;
  roomId = code || genRoomCode();
  $("roomTitle").textContent = `🔴 Live`;
  hideAll();
  await acquireLocalStream();
  assignSlot(currentUser.uid, myDisplayName + " (you)", localStream, true);
  showCtrlBar();
  toast(`🔴 Live started — your followers can join from the Feed!`);
  listenForJoinRequests();
  setupRTDB();
  startHostStreamGuard();
}

// ─────────────────────────────────────────────────────────────────
// Go Live button (host must press to officially start broadcast)
// ─────────────────────────────────────────────────────────────────
async function handleGoLive() {
  if (!isHost) return;
  if (!localStream) await acquireLocalStream();
  liveActive = true;
  $("btnGoLive").style.display  = "none";
  $("btnEndLive").style.display = "";
  $("live-badge").classList.add("visible");
  $("viewer-count").style.display = "flex";
  await setDoc(doc(db, "liveRooms", roomId), {
    host:        currentUser.uid,
    hostName:    myDisplayName,
    roomId,
    live:        true,
    locked:      false,
    createdAt:   serverTimestamp(),
    viewerCount: 0
  });
  listenForJoinRequests();
  listenViewerCount();
  toast("🔴 You are now Live! Your followers can see you on their Feed.");
}

async function handleEndLive() {
  if (!isHost) return;
  if (!confirm("End the Live for everyone?")) return;
  await endLive();
}

async function endLive() {
  liveActive = false;
  stopGuestConnectionMonitor();
  Object.keys(guests).forEach(uid => closePeer(uid));
  if (roomId) {
    // Mark live ended in the liveRooms collection
    try { await updateDoc(doc(db, "liveRooms", roomId), { live: false, endedAt: serverTimestamp() }); }
    catch (_) { /* best-effort */ }
    // Also mark liveActive:false in the stories collection so the feed card disappears
    try { await updateDoc(doc(db, "stories", roomId), { liveActive: false }); }
    catch (_) { /* best-effort — may not exist for legacy rooms */ }
  }
  if (presenceRef) { set(presenceRef, null).catch(() => {}); }
  localStream?.getTracks().forEach(t => t.stop());
  localStream = null;
  $("live-badge").classList.remove("visible");
  $("btnGoLive").style.display  = "";
  $("btnEndLive").style.display = "none";
  buildVideoGrid();
  showLobby();
  $("roomTitle").textContent = "Shadow Nexus Live";
}

// ─────────────────────────────────────────────────────────────────
// Guest join flow
// ─────────────────────────────────────────────────────────────────
function handleJoinConfirm() {
  const code = $("room-code-input").value.trim().toUpperCase().slice(0, 6);
  if (code.length < 4) { toast("Enter a valid room code"); return; }
  requestToJoin(code);
}

async function requestToJoin(code) {
  roomId = code;
  $("roomTitle").textContent = `🔴 Live`;
  hideAll();
  $("waiting-overlay").classList.remove("hidden");
  $("waitingSub").textContent = `Waiting for host approval…`;

  try {
    await acquireLocalStream();
  } catch (e) {
    // Show clear error, stay on waiting screen
    $("waitingSub").textContent = getMediaErrorMessage(e);
    return;
  }
  setupRTDB();

  await setDoc(doc(db, "liveRooms", roomId, "requests", currentUser.uid), {
    uid:         currentUser.uid,
    displayName: myDisplayName,
    status:      "pending",
    requestedAt: serverTimestamp()
  });

  const reqRef   = doc(db, "liveRooms", roomId, "requests", currentUser.uid);
  const unsub    = onSnapshot(reqRef, snap => {
    if (!snap.exists()) return;
    const status = snap.data().status;
    if (status === "accepted") {
      unsub();
      hideOverlay("waiting-overlay");
      joinAsGuest();
    } else if (status === "denied") {
      unsub();
      localStream?.getTracks().forEach(t => t.stop());
      localStream = null;
      showLobby();
      toast("Host denied your request.");
    }
  });
  _unsubs.push(unsub);
}

async function cancelJoinRequest() {
  if (roomId && currentUser) {
    deleteDoc(doc(db, "liveRooms", roomId, "requests", currentUser.uid)).catch(() => {});
  }
  localStream?.getTracks().forEach(t => t.stop());
  localStream = null;
  hideOverlay("waiting-overlay");
  showLobby();
}

async function joinAsGuest() {
  liveActive = true;
  assignSlot(currentUser.uid, myDisplayName + " (you)", localStream, false);
  showCtrlBar();
  await initiateGuestPeerConnection(currentUser.uid);
  listenViewerCount();
  listenChat();
  listenForHostCommands();
  startGuestConnectionMonitor();
}

// ─────────────────────────────────────────────────────────────────
// Guest self-recovery heartbeat — checks own PC every 8 s
// If connection is lost/failed and we have retries left, re-initiate
// ─────────────────────────────────────────────────────────────────
function startGuestConnectionMonitor() {
  if (_connMonInterval) clearInterval(_connMonInterval);
  _connMonInterval = setInterval(async () => {
    if (!liveActive) { clearInterval(_connMonInterval); return; }
    const g = guests[currentUser.uid];
    if (!g) return;
    const state = g.pc?.connectionState;
    const ice   = g.pc?.iceConnectionState;
    // Transient weak signal → just update status dot, no reconnect yet
    if (ice === "disconnected") {
      setBoxStatus(currentUser.uid, "weak");
      toast("🟡 Weak connection — keeping you in the Live…");
      return;
    }
    // Full failure → attempt self-recovery
    if ((state === "failed" || state === "disconnected") && g.retries < 8) {
      g.retries++;
      const delay = Math.min(1000 * 2 ** g.retries, 20000);
      setBoxStatus(currentUser.uid, "reconnecting");
      showReconnectBanner();
      toast(`Connection lost. Reconnecting… (${g.retries}/8)`);
      setTimeout(async () => {
        try {
          // Try ICE restart first (faster, no new offer needed)
          const offer = await g.pc.createOffer({ iceRestart: true });
          await g.pc.setLocalDescription(offer);
          await setDoc(doc(db, "liveRooms", roomId, "signals", currentUser.uid), {
            offer: offer.toJSON(), ts: serverTimestamp()
          });
        } catch (_) {
          // Full re-initiate as fallback
          try {
            g.pc.close();
          } catch (_) {}
          delete guests[currentUser.uid];
          await initiateGuestPeerConnection(currentUser.uid);
        }
      }, delay);
    }
  }, 8000);
}

function stopGuestConnectionMonitor() {
  if (_connMonInterval) { clearInterval(_connMonInterval); _connMonInterval = null; }
}

// ─────────────────────────────────────────────────────────────────
// Host stream guard — ensures host box stays alive even when guests churn
// ─────────────────────────────────────────────────────────────────
function startHostStreamGuard() {
  setInterval(() => {
    if (!liveActive || !isHost) return;
    const mySlot = slotFor(currentUser.uid);
    if (!mySlot) return;
    // If our own local video element lost its stream, re-attach it
    const vid = mySlot.querySelector("video");
    if (localStream && (!vid.srcObject || vid.srcObject !== localStream)) {
      vid.srcObject = localStream;
      vid.play().catch(() => {});
    }
    // If local video tracks are all ended, restart stream
    const vTracks = localStream?.getVideoTracks() || [];
    if (vTracks.length > 0 && vTracks.every(t => t.readyState === "ended")) {
      restartLocalStream().catch(() => {});
    }
  }, 6000);
}

// ─────────────────────────────────────────────────────────────────
// Host: listen for join requests
// ─────────────────────────────────────────────────────────────────
function listenForJoinRequests() {
  const qRef = collection(db, "liveRooms", roomId, "requests");
  const unsub = onSnapshot(qRef, snap => {
    snap.docChanges().forEach(change => {
      if (change.type === "added") {
        const req = change.doc.data();
        if (req.status === "pending") renderJoinRequest(req);
      }
      if (change.type === "removed") {
        removeRequestCard(change.doc.id);
      }
    });
    const pending = snap.docs.filter(d => d.data().status === "pending").length;
    const badge = $("req-badge");
    if (badge) { badge.textContent = pending || ""; badge.classList.toggle("has-items", pending > 0); }
  });
  _unsubs.push(unsub);
}

function renderJoinRequest(req) {
  const panel = $("requests-panel");
  if (!panel || panel.querySelector(`[data-uid="${req.uid}"]`)) return;
  const card = el("div", "request-card");
  card.dataset.uid = req.uid;
  card.innerHTML = `
    <div class="request-avatar">👤</div>
    <div class="request-name">${esc(req.displayName)}</div>
    <div class="request-btns">
      <button class="req-accept">✓ Accept</button>
      <button class="req-deny">✕ Deny</button>
    </div>`;
  card.querySelector(".req-accept").onclick = () => acceptGuest(req);
  card.querySelector(".req-deny").onclick   = () => denyGuest(req.uid);
  panel.appendChild(card);
}

function removeRequestCard(uid) {
  document.querySelector(`.request-card[data-uid="${uid}"]`)?.remove();
}

async function acceptGuest(req) {
  if (Object.keys(guests).length >= 7) { toast("Max 7 guests reached."); return; }
  if (roomLocked) { toast("Room is locked."); return; }
  removeRequestCard(req.uid);
  await updateDoc(doc(db, "liveRooms", roomId, "requests", req.uid), { status: "accepted" });
  createHostPeer(req.uid, req.displayName);
}

async function denyGuest(uid) {
  removeRequestCard(uid);
  await updateDoc(doc(db, "liveRooms", roomId, "requests", uid), { status: "denied" });
}

// ─────────────────────────────────────────────────────────────────
// WebRTC — Host creates a peer per guest (fully isolated try/catch)
// ─────────────────────────────────────────────────────────────────
async function createHostPeer(guestUid, displayName) {
  // Each peer is wrapped in its own try/catch so one failure never affects others
  try {
    const pc = newPC();
    guests[guestUid] = { pc, stream: null, displayName, muted: false, camOff: false, retries: 0, quality: "HIGH" };

    localStream.getTracks().forEach(t => pc.addTrack(t, localStream));

    const sigRef = doc(db, "liveRooms", roomId, "signals", guestUid);
    pc.onicecandidate = async e => {
      if (e.candidate) {
        await addDoc(collection(db, "liveRooms", roomId, "signals", guestUid, "hostIce"), { c: e.candidate.toJSON() });
      }
    };

    pc.ontrack = e => {
      try {
        guests[guestUid].stream = e.streams[0];
        const slot = slotFor(guestUid) || assignSlot(guestUid, displayName, e.streams[0], false);
        if (slot) {
          const vid = slot.querySelector("video");
          vid.srcObject = e.streams[0];
          vid.play().catch(() => {});
        }
        updateMiniStrip();
      } catch (_) {} // isolate
    };

    pc.onconnectionstatechange = () => { try { handlePCState(pc, guestUid); } catch (_) {} };
    pc.oniceconnectionstatechange = () => { try { monitorIceState(pc, guestUid); } catch (_) {} };

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    await setDoc(sigRef, { offer: offer.toJSON(), guestName: displayName, ts: serverTimestamp() });

    const unsubAns = onSnapshot(sigRef, async snap => {
      try {
        if (!snap.exists()) return;
        const data = snap.data();
        if (data.answer && pc.signalingState === "have-local-offer") {
          await pc.setRemoteDescription(data.answer).catch(() => {});
          unsubAns();
        }
      } catch (_) {}
    });
    _unsubs.push(unsubAns);

    const iceRef  = collection(db, "liveRooms", roomId, "signals", guestUid, "guestIce");
    const unsubIce = onSnapshot(iceRef, snap => {
      snap.docChanges().forEach(ch => {
        if (ch.type === "added") {
          pc.addIceCandidate(ch.doc.data().c).catch(() => {});
        }
      });
    });
    _unsubs.push(unsubIce);

    assignSlot(guestUid, displayName, null, false);
    startQualityMonitor(guestUid);
  } catch (err) {
    // This guest's peer failed to set up — clear only their slot
    toast(`Could not connect ${displayName}. Try restarting their box.`);
    const slot = slotFor(guestUid);
    if (slot) setBoxStatus(slot, "error", "Connection failed.");
    delete guests[guestUid];
  }
}

// ─────────────────────────────────────────────────────────────────
// WebRTC — Guest creates peer and responds to host offer (isolated)
// ─────────────────────────────────────────────────────────────────
async function initiateGuestPeerConnection(guestUid) {
  try {
    const pc = newPC();
    guests[guestUid] = { pc, stream: localStream, displayName: myDisplayName, muted: false, camOff: false, retries: 0, quality: "HIGH" };

    localStream.getTracks().forEach(t => pc.addTrack(t, localStream));

    const sigRef = doc(db, "liveRooms", roomId, "signals", guestUid);

    pc.onicecandidate = async e => {
      if (e.candidate) {
        await addDoc(collection(db, "liveRooms", roomId, "signals", guestUid, "guestIce"), { c: e.candidate.toJSON() });
      }
    };

    pc.ontrack = e => {
      try {
        const slot = document.querySelector(".video-box.host-box");
        if (slot) { const vid = slot.querySelector("video"); vid.srcObject = e.streams[0]; vid.play().catch(() => {}); }
      } catch (_) {}
    };

    pc.onconnectionstatechange = () => { try { handlePCState(pc, guestUid); } catch (_) {} };
    pc.oniceconnectionstatechange = () => { try { monitorIceState(pc, guestUid); } catch (_) {} };

    const unsubOffer = onSnapshot(sigRef, async snap => {
      try {
        if (!snap.exists()) return;
        const data = snap.data();
        if (data.offer && pc.signalingState === "stable") {
          await pc.setRemoteDescription(data.offer).catch(() => {});
          const answer = await pc.createAnswer();
          await pc.setLocalDescription(answer);
          await updateDoc(sigRef, { answer: answer.toJSON() });
          unsubOffer();
        }
      } catch (_) {}
    });
    _unsubs.push(unsubOffer);

    const iceRef   = collection(db, "liveRooms", roomId, "signals", guestUid, "hostIce");
    const unsubIce = onSnapshot(iceRef, snap => {
      snap.docChanges().forEach(ch => {
        if (ch.type === "added") {
          pc.addIceCandidate(ch.doc.data().c).catch(() => {});
        }
      });
    });
    _unsubs.push(unsubIce);

    listenChat();
  } catch (err) {
    const mySlot = slotFor(guestUid);
    if (mySlot) setBoxStatus(mySlot, "error", "Connection failed.");
    toast("Connection failed. Try refreshing.");
  }
}

// ─────────────────────────────────────────────────────────────────
// Create a configured RTCPeerConnection
// bundlePolicy + rtcpMuxPolicy reduce media lines → less overhead on weak links
// ─────────────────────────────────────────────────────────────────
function newPC() {
  return new RTCPeerConnection({
    iceServers:           ICE_SERVERS,
    iceCandidatePoolSize: 10,
    bundlePolicy:         "max-bundle",
    rtcpMuxPolicy:        "require",
  });
}

// ─────────────────────────────────────────────────────────────────
// ICE / connection state monitoring + auto-reconnect (per-box isolated)
// ─────────────────────────────────────────────────────────────────
function handlePCState(pc, uid) {
  const state = pc.connectionState;
  const g = guests[uid];
  if (!g) return;

  if (state === "connecting" || state === "checking") {
    setBoxStatus(uid, "reconnecting");
  }

  if (state === "failed" || state === "disconnected") {
    const MAX_RETRIES = 8;
    if (g.retries < MAX_RETRIES) {
      g.retries++;
      const delay = Math.min(1000 * 2 ** (g.retries - 1), 20000);
      setBoxStatus(uid, "reconnecting");
      showReconnectBanner();
      if (uid === currentUser?.uid) {
        toast(`Connection lost. Reconnecting… (${g.retries}/${MAX_RETRIES})`);
      } else {
        toast(`🔄 Reconnecting ${g.displayName}… (${g.retries}/${MAX_RETRIES})`);
      }
      setTimeout(() => reconnectPeer(uid), delay);
    } else {
      // Max retries reached — show error but do NOT crash other boxes
      setBoxStatus(uid, "error", "Connection lost. Tap 🔄 to retry.");
      // Only clear slot for guest peers, not our own box
      if (uid !== currentUser?.uid) closePeer(uid);
      toast(`${g.displayName} disconnected.`);
    }
  }

  if (state === "connected") {
    g.retries = 0;
    setBoxStatus(uid, "good");
    hideReconnectBanner();
    if (uid === currentUser?.uid) toast("✅ Reconnected!");
  }
}

function monitorIceState(pc, uid) {
  if (pc.iceConnectionState === "failed") handlePCState(pc, uid);
  if (pc.iceConnectionState === "disconnected") {
    // Transient disconnection — show 🟡 weak but don't reconnect yet
    setBoxStatus(uid, "weak");
    if (uid === currentUser?.uid) toast("🟡 Weak connection — holding your spot…");
  }
  if (pc.iceConnectionState === "connected" || pc.iceConnectionState === "completed") {
    setBoxStatus(uid, "good");
  }
}

async function reconnectPeer(uid) {
  const g = guests[uid];
  if (!g) return;
  setBoxStatus(uid, "reconnecting");
  try {
    const offer = await g.pc.createOffer({ iceRestart: true });
    await g.pc.setLocalDescription(offer);
    await setDoc(doc(db, "liveRooms", roomId, "signals", uid), { offer: offer.toJSON(), ts: serverTimestamp() });
  } catch (_) {
    // ICE restart failed — clear only this peer's slot, never touch others
    setBoxStatus(uid, "error", "Connection lost. Tap 🔄 to retry.");
    closePeer(uid);
  }
}

function closePeer(uid) {
  const g = guests[uid];
  if (!g) return;
  try { g.pc.close(); } catch (_) {}
  if (g._qualityInterval) clearInterval(g._qualityInterval);
  delete guests[uid];
  clearSlot(uid);
  updateMiniStrip();
  deleteDoc(doc(db, "liveRooms", roomId, "signals", uid)).catch(() => {});
}

// ─────────────────────────────────────────────────────────────────
// Global reconnect banner (shown if ANY peer is reconnecting)
// ─────────────────────────────────────────────────────────────────
function showReconnectBanner() { $("reconnect-banner").classList.add("visible"); }
function hideReconnectBanner() {
  // Only hide when no box is in reconnecting state
  const anyReconnecting = Object.values(guests).some(g =>
    g.pc?.connectionState === "disconnected" || g.pc?.connectionState === "failed"
  );
  if (!anyReconnecting) $("reconnect-banner").classList.remove("visible");
}

// ─────────────────────────────────────────────────────────────────
// Local media acquisition — with clear error messages
// ─────────────────────────────────────────────────────────────────
async function acquireLocalStream(constraints) {
  // Start at a quality appropriate for the detected network
  const cappedLevel = capQualityByNetwork(currentQuality);
  if (cappedLevel !== currentQuality) currentQuality = cappedLevel;
  const qual = QUALITY[currentQuality];
  const c = constraints || {
    video: {
      width:     { ideal: qual.width  },
      height:    { ideal: qual.height },
      frameRate: { ideal: qual.frameRate },
      facingMode
    },
    audio: {
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl:  true,
      sampleRate:       44100
    }
  };
  try {
    localStream = await navigator.mediaDevices.getUserMedia(c);
  } catch (e) {
    // Resolution may not be supported — fall back to unconstrained video
    if (e.name === "OverconstrainedError") {
      try {
        localStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode }, audio: { echoCancellation: true, noiseSuppression: true } });
      } catch (e2) {
        const msg = getMediaErrorMessage(e2);
        toast(msg);
        const mySlot = slotFor(currentUser?.uid);
        if (mySlot) setBoxStatus(mySlot, "error", msg);
        throw e2;
      }
    } else {
      const msg = getMediaErrorMessage(e);
      toast(msg);
      const mySlot = slotFor(currentUser?.uid);
      if (mySlot) setBoxStatus(mySlot, "error", msg);
      throw e;
    }
  }
  if (isMobile()) {
    navigator.mediaDevices.enumerateDevices().then(devs => {
      const cams = devs.filter(d => d.kind === "videoinput");
      if (cams.length > 1) $("btnFlip").style.display = "";
    });
  }
  startVAD();
  return localStream;
}

// Human-readable messages for every media error type
function getMediaErrorMessage(e) {
  if (!e) return "Camera/mic error.";
  const n = e.name || "";
  if (n === "NotAllowedError"  || n === "PermissionDeniedError") return "Microphone permission denied. Please allow access.";
  if (n === "NotFoundError"    || n === "DevicesNotFoundError")  return "Camera unavailable. No device found.";
  if (n === "NotReadableError" || n === "TrackStartError")       return "Camera is already in use by another app.";
  if (n === "OverconstrainedError")                              return "Camera does not support the requested resolution.";
  if (n === "TypeError")                                         return "No media devices found on this browser.";
  return "Camera/mic access denied. Check browser permissions.";
}

// ─────────────────────────────────────────────────────────────────
// Controls
// ─────────────────────────────────────────────────────────────────
function toggleMic() {
  micEnabled = !micEnabled;
  localStream?.getAudioTracks().forEach(t => { t.enabled = micEnabled; });
  $("btnMic").classList.toggle("active", !micEnabled);
  $("btnMic").innerHTML = (micEnabled ? "🎙️" : "🔇") + '<span class="ctrl-tooltip">' + (micEnabled ? "Mute" : "Unmute") + "</span>";
  updateLocalBadges();
}

function toggleCam() {
  camEnabled = !camEnabled;
  localStream?.getVideoTracks().forEach(t => { t.enabled = camEnabled; });
  $("btnCam").classList.toggle("active", !camEnabled);
  $("btnCam").innerHTML = (camEnabled ? "📷" : "🚫") + '<span class="ctrl-tooltip">' + (camEnabled ? "Camera" : "Cam off") + "</span>";
  const mySlot = slotFor(currentUser.uid);
  if (mySlot) mySlot.classList.toggle("cam-off", !camEnabled);
  updateLocalBadges();
}

async function flipCamera() {
  facingMode = facingMode === "user" ? "environment" : "user";
  const old = localStream;
  old?.getTracks().forEach(t => t.stop());
  await acquireLocalStream();
  const newVid = localStream.getVideoTracks()[0];
  Object.values(guests).forEach(g => {
    const sender = g.pc.getSenders().find(s => s.track?.kind === "video");
    if (sender) sender.replaceTrack(newVid).catch(() => {});
  });
  const mySlot = slotFor(currentUser.uid);
  if (mySlot) { const vid = mySlot.querySelector("video"); vid.srcObject = localStream; }
}

function toggleLock() {
  roomLocked = !roomLocked;
  $("btnLock").innerHTML = (roomLocked ? "🔒" : "🔓") + '<span class="ctrl-tooltip">' + (roomLocked ? "Locked" : "Lock room") + "</span>";
  if (roomId) updateDoc(doc(db, "liveRooms", roomId), { locked: roomLocked }).catch(() => {});
  toast(roomLocked ? "Room locked — no new guests." : "Room unlocked.");
}

function updateLocalBadges() {
  const slot = slotFor(currentUser.uid);
  if (!slot) return;
  const bads = slot.querySelector(".box-badges");
  bads.innerHTML = "";
  if (!micEnabled) { const b = el("div", "badge-icon muted", "🔇"); bads.appendChild(b); }
  if (!camEnabled) { const b = el("div", "badge-icon cam-off", "🚫"); bads.appendChild(b); }
}

// Host mute/cam-off a guest remotely via Firestore command
async function hostMuteGuest(uid) {
  await setDoc(doc(db, "liveRooms", roomId, "commands", uid), { cmd: "mute", from: currentUser.uid, ts: serverTimestamp() });
  const slot = slotFor(uid);
  if (slot) {
    const bads = slot.querySelector(".box-badges");
    const b = el("div", "badge-icon muted", "🔇"); bads.appendChild(b);
  }
}
async function hostDisableCam(uid) {
  await setDoc(doc(db, "liveRooms", roomId, "commands", uid), { cmd: "camOff", from: currentUser.uid, ts: serverTimestamp() });
}
async function hostRemoveGuest(uid) {
  await setDoc(doc(db, "liveRooms", roomId, "commands", uid), { cmd: "remove", from: currentUser.uid, ts: serverTimestamp() });
  closePeer(uid);
}

// Guest listens for commands from host
function listenForHostCommands() {
  const cmdRef = doc(db, "liveRooms", roomId, "commands", currentUser.uid);
  const unsub  = onSnapshot(cmdRef, snap => {
    if (!snap.exists()) return;
    const { cmd } = snap.data();
    if (cmd === "mute"   && micEnabled) toggleMic();
    if (cmd === "camOff" && camEnabled) toggleCam();
    if (cmd === "remove") {
      closePeer(currentUser.uid);
      localStream?.getTracks().forEach(t => t.stop());
      showLobby();
      toast("You were removed from the Live.");
    }
    deleteDoc(cmdRef).catch(() => {});
  });
  _unsubs.push(unsub);
}

// ─────────────────────────────────────────────────────────────────
// Adaptive bitrate monitor (runs every 5 s per peer, fully isolated)
// ─────────────────────────────────────────────────────────────────
function startQualityMonitor(uid) {
  const interval = setInterval(async () => {
    const g = guests[uid];
    if (!g) { clearInterval(interval); return; }
    try {
      const stats = await g.pc.getStats();
      let rtt = 0, lost = 0, bytesSent = 0;
      stats.forEach(r => {
        if (r.type === "remote-inbound-rtp" && r.kind === "video") {
          rtt  = (r.roundTripTime || 0) * 1000;
          lost = r.fractionLost || 0;
        }
        if (r.type === "outbound-rtp" && r.kind === "video") {
          bytesSent = r.bytesSent || 0;
        }
      });
      let target = decideQuality(rtt, lost);
      // Cap to network tier
      target = capQualityByNetwork(target);
      if (target !== g.quality) {
        g.quality = target;
        applyQualityToSender(g.pc, target).catch(() => {});
        updateQualityDot(uid, target);
        // Update status bar + user-visible toast for own box
        if (target === "VERY_LOW") {
          setBoxStatus(uid, "weak");
          if (uid === currentUser?.uid) toast("🟡 Very weak signal — audio-priority mode on");
        } else if (target === "LOW") {
          setBoxStatus(uid, "weak");
          if (uid === currentUser?.uid) toast("🟡 Weak connection — reducing video quality");
        } else if (target === "MEDIUM") {
          setBoxStatus(uid, "weak");
        } else {
          setBoxStatus(uid, "good");
          if (uid === currentUser?.uid && g._wasWeak) toast("✅ Connection improved");
        }
        g._wasWeak = (target !== "HIGH");
      }
    } catch (_) {} // isolate — one bad peer never stops others
  }, 5000);
  if (guests[uid]) guests[uid]._qualityInterval = interval;
}

function decideQuality(rtt, loss) {
  if (rtt > QUALITY_THRESHOLDS.rttExtreme  || loss > QUALITY_THRESHOLDS.lossExtreme)  return "VERY_LOW";
  if (rtt > QUALITY_THRESHOLDS.rttCritical || loss > QUALITY_THRESHOLDS.lossCritical) return "LOW";
  if (rtt > QUALITY_THRESHOLDS.rttHigh     || loss > QUALITY_THRESHOLDS.lossHigh)     return "MEDIUM";
  return "HIGH";
}

async function applyQualityToSender(pc, level) {
  const q = QUALITY[level];
  // ── Video sender ──────────────────────────────────────────────
  const videoSender = pc.getSenders().find(s => s.track?.kind === "video");
  if (videoSender) {
    try {
      const params = videoSender.getParameters();
      if (!params.encodings?.length) params.encodings = [{}];
      params.encodings[0].maxBitrate   = q.bitrate;
      params.encodings[0].maxFramerate = q.frameRate;
      // VERY_LOW: disable video track entirely when signal is critical
      if (level === "VERY_LOW") {
        videoSender.track.enabled = false;
      } else {
        videoSender.track.enabled = true;
        await videoSender.setParameters(params);
        await videoSender.track.applyConstraints({
          width: q.width, height: q.height, frameRate: q.frameRate
        }).catch(() => {});
      }
    } catch (_) {}
  }
  // ── Audio sender — always applied, bitrate is preserved last ──
  const audioSender = pc.getSenders().find(s => s.track?.kind === "audio");
  if (audioSender) {
    try {
      const ap = audioSender.getParameters();
      if (!ap.encodings?.length) ap.encodings = [{}];
      ap.encodings[0].maxBitrate = q.audioBitrate;
      await audioSender.setParameters(ap);
    } catch (_) {}
  }
}

function updateQualityDot(uid, level) {
  const slot = slotFor(uid);
  if (!slot) return;
  const dot = slot.querySelector(".quality-dot");
  if (!dot) return;
  dot.className = "quality-dot " + { HIGH: "good", MEDIUM: "ok", LOW: "poor", VERY_LOW: "poor" }[level];
}

// ─────────────────────────────────────────────────────────────────
// VAD — active speaker detection via Web Audio
// ─────────────────────────────────────────────────────────────────
let _vadCtx = null;
let _vadRunning = false;
function startVAD() {
  if (!localStream || _vadCtx) return;
  try {
    _vadCtx = new (window.AudioContext || window.webkitAudioContext)();
    const src      = _vadCtx.createMediaStreamSource(localStream);
    const analyser = _vadCtx.createAnalyser();
    analyser.fftSize = 512;
    src.connect(analyser);
    const buf = new Uint8Array(analyser.frequencyBinCount);
    _vadRunning = true;
    function tick() {
      if (!_vadRunning) return;
      requestAnimationFrame(tick);
      analyser.getByteFrequencyData(buf);
      const vol = buf.reduce((a, b) => a + b, 0) / buf.length;
      const slot = slotFor(currentUser.uid);
      if (slot) slot.classList.toggle("speaking", vol > 18);
    }
    tick();
  } catch (_) { /* Safari / old browsers */ }
}

// ─────────────────────────────────────────────────────────────────
// Firebase Realtime DB — viewer count, chat, reactions
// ─────────────────────────────────────────────────────────────────
function setupRTDB() {
  if (!roomId) return;
  roomRtRef   = ref(rtdb, `liveRooms/${roomId}`);
  chatRtRef   = ref(rtdb, `liveRooms/${roomId}/chat`);
  viewerRef   = ref(rtdb, `liveRooms/${roomId}/viewers/${currentUser.uid}`);
  presenceRef = viewerRef;

  set(viewerRef, { uid: currentUser.uid, name: myDisplayName, ts: Date.now() });
  onDisconnect(viewerRef).remove();

  listenViewerCount();
  listenChat();
}

function listenViewerCount() {
  if (!roomRtRef) return;
  const vRef = ref(rtdb, `liveRooms/${roomId}/viewers`);
  onValue(vRef, snap => {
    const count = snap.exists() ? Object.keys(snap.val() || {}).length : 0;
    $("viewerNum").textContent = count;
  });
}

function listenChat() {
  if (!chatRtRef) return;
  const q = query(collection(db, "liveRooms", roomId, "chat"), orderBy("ts", "asc"), limit(200));
  const unsub = onSnapshot(q, snap => {
    snap.docChanges().forEach(ch => {
      if (ch.type === "added") appendChatMsg(ch.doc.data());
    });
  });
  _unsubs.push(unsub);
}

async function sendChat() {
  const input = $("chat-input");
  const text  = input.value.trim();
  if (!text || !roomId) return;
  input.value = "";
  await addDoc(collection(db, "liveRooms", roomId, "chat"), {
    uid:  currentUser.uid,
    name: myDisplayName,
    text,
    isHost: isHost,
    ts:   serverTimestamp()
  });
}

function sendChatMobile() {
  const input = $("chat-input-mobile");
  const text  = input.value.trim();
  if (!text || !roomId) return;
  input.value = "";
  addDoc(collection(db, "liveRooms", roomId, "chat"), {
    uid:  currentUser.uid,
    name: myDisplayName,
    text,
    isHost: isHost,
    ts:   serverTimestamp()
  });
}

function appendChatMsg(data) {
  const msgEl = el("div", data.isReaction ? "chat-msg reaction-msg" : "chat-msg",
    data.isReaction
      ? data.text
      : `<span class="msg-name${data.isHost ? " host" : ""}">${esc(data.name)}</span>: <span class="msg-text">${esc(data.text)}</span>`
  );
  const panels = [$("chat-messages"), $("mobile-chat-messages")];
  panels.forEach(p => {
    if (!p) return;
    const clone = msgEl.cloneNode(true);
    p.appendChild(clone);
    p.scrollTop = p.scrollHeight;
  });
}

function sendReaction(emoji) {
  if (!roomId) return;
  addDoc(collection(db, "liveRooms", roomId, "chat"), {
    uid: currentUser.uid, name: myDisplayName,
    text: emoji, isReaction: true, ts: serverTimestamp()
  });
  flyReaction(emoji);
}

function flyReaction(emoji) {
  const stage = $("reaction-stage");
  const r = el("div", "fly-reaction", emoji);
  r.style.left = (Math.random() * 20 - 10) + "px";
  stage.appendChild(r);
  setTimeout(() => r.remove(), 2500);
}

// ─────────────────────────────────────────────────────────────────
// Host context menu (long-press / right-click guest box)
// ─────────────────────────────────────────────────────────────────
let _ctxUid = null;
let _pressTimer = null;

function addContextMenuTrigger(box) {
  const show = (uid, x, y) => {
    if (!isHost || uid === currentUser.uid) return;
    _ctxUid = uid;
    const menu = $("guest-ctx-menu");
    menu.style.left = Math.min(x, window.innerWidth  - 180) + "px";
    menu.style.top  = Math.min(y, window.innerHeight - 200) + "px";
    menu.classList.add("visible");
  };
  box.addEventListener("contextmenu", e => {
    e.preventDefault();
    const uid = box.dataset.uid;
    if (uid) show(uid, e.clientX, e.clientY);
  });
  box.addEventListener("touchstart", e => {
    const uid = box.dataset.uid;
    if (!uid) return;
    _pressTimer = setTimeout(() => show(uid, e.touches[0].clientX, e.touches[0].clientY), 600);
  }, { passive: true });
  box.addEventListener("touchend", () => clearTimeout(_pressTimer));
}

function hideCtxMenu() { $("guest-ctx-menu").classList.remove("visible"); _ctxUid = null; }

$("ctx-mute").onclick    = () => { if (_ctxUid) hostMuteGuest(_ctxUid);    hideCtxMenu(); };
$("ctx-cam").onclick     = () => { if (_ctxUid) hostDisableCam(_ctxUid);   hideCtxMenu(); };
$("ctx-remove").onclick  = () => { if (_ctxUid) hostRemoveGuest(_ctxUid);  hideCtxMenu(); };
$("ctx-restart").onclick = () => { if (_ctxUid) { reconnectPeer(_ctxUid); toast(`Restarting ${guests[_ctxUid]?.displayName || "guest"}…`); } hideCtxMenu(); };

// ─────────────────────────────────────────────────────────────────
// Mobile mini-strip — pause hidden video previews to save battery
// ─────────────────────────────────────────────────────────────────
function updateMiniStrip() {
  if (!isMobile()) return;
  const strip = $("mini-strip");
  strip.innerHTML = "";
  Object.entries(guests).forEach(([uid, g]) => {
    if (uid === currentUser.uid) return;
    const box = el("div", "mini-box", "");
    box.dataset.uid = uid;
    const vid = document.createElement("video");
    vid.autoplay = true; vid.playsInline = true; vid.muted = true;
    // Only set srcObject for active speaker on mobile (saves battery)
    if (g.stream) vid.srcObject = g.stream;
    box.appendChild(vid);
    const nm = el("div", "mini-name", esc(g.displayName));
    box.appendChild(nm);
    box.onclick = () => setActiveSpeaker(uid);
    strip.appendChild(box);
  });
  // Pause all mini videos that are not the active speaker (battery saving)
  pauseInactiveMiniVideos();
}

function pauseInactiveMiniVideos() {
  if (!isMobile()) return;
  document.querySelectorAll(".mini-box video").forEach(vid => {
    const box = vid.closest(".mini-box");
    const uid = box?.dataset.uid;
    const activeSlot = document.querySelector('.video-box[data-active="true"]');
    const isActive = activeSlot && activeSlot.dataset.uid === uid;
    if (isActive) {
      vid.play().catch(() => {});
    } else {
      // Pause video to save CPU/battery; keep srcObject so it can resume
      vid.pause();
    }
  });
}

function setActiveSpeaker(uid) {
  document.querySelectorAll(".video-box").forEach(b => { delete b.dataset.active; });
  const slot = slotFor(uid);
  if (slot) slot.dataset.active = "true";
  pauseInactiveMiniVideos();
}

// ─────────────────────────────────────────────────────────────────
// Side tab switching
// ─────────────────────────────────────────────────────────────────
function switchSideTab(tab) {
  document.querySelectorAll(".side-tab").forEach(b => b.classList.toggle("active", b.dataset.tab === tab));
  $("chat-panel")?.classList.toggle("active",     tab === "chat");
  $("requests-panel")?.classList.toggle("active", tab === "requests");
}
window.switchSideTab = switchSideTab;

// ─────────────────────────────────────────────────────────────────
// Mobile chat drawer
// ─────────────────────────────────────────────────────────────────
function toggleMobileChat() {
  $("mobile-chat-drawer").classList.toggle("open");
}
window.toggleMobileChat = toggleMobileChat;

// ─────────────────────────────────────────────────────────────────
// Ctrl bar visibility
// ─────────────────────────────────────────────────────────────────
function showCtrlBar() {
  $("ctrl-bar").classList.add("visible");
  if (isMobile()) $("mobile-chat-btn").style.display = "flex";
}

// ─────────────────────────────────────────────────────────────────
// Back / exit
// ─────────────────────────────────────────────────────────────────
async function handleBack() {
  if (liveActive) {
    if (!confirm("Leave the Live?")) return;
    if (isHost) await endLive(); else leaveAsGuest();
  }
  window.location.href = "index.html";
}

async function leaveAsGuest() {
  stopGuestConnectionMonitor();
  Object.keys(guests).forEach(uid => closePeer(uid));
  localStream?.getTracks().forEach(t => t.stop());
  localStream = null;
  _vadRunning = false;
  if (_vadCtx) { try { _vadCtx.close(); } catch (_) {} _vadCtx = null; }
  if (presenceRef) set(presenceRef, null).catch(() => {});
  if (roomId && currentUser) {
    deleteDoc(doc(db, "liveRooms", roomId, "requests", currentUser.uid)).catch(() => {});
    deleteDoc(doc(db, "liveRooms", roomId, "signals",  currentUser.uid)).catch(() => {});
  }
  _unsubs.forEach(u => u()); _unsubs.length = 0;
}

// ─────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────
function esc(s) {
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}
function isMobile() { return window.innerWidth <= 700; }

function toast(msg, dur = 3500) {
  const t = $("live-toast");
  t.textContent = msg;
  t.classList.add("show");
  setTimeout(() => t.classList.remove("show"), dur);
}

// ─────────────────────────────────────────────────────────────────
// Expose globals needed by inline HTML onclick handlers
// ─────────────────────────────────────────────────────────────────
window.sendReaction   = sendReaction;
window.sendChatMobile = sendChatMobile;

// ─────────────────────────────────────────────────────────────────
// Mobile orientation change — re-acquire stream at new resolution
// ─────────────────────────────────────────────────────────────────
window.addEventListener("orientationchange", async () => {
  if (!localStream || !liveActive) return;
  await new Promise(r => setTimeout(r, 400));
  const newTrack = localStream.getVideoTracks()[0];
  if (newTrack) {
    const q = QUALITY[currentQuality];
    await newTrack.applyConstraints({ width: { ideal: q.width }, height: { ideal: q.height } }).catch(() => {});
  }
});

// ─────────────────────────────────────────────────────────────────
// Page visibility — pause ALL video tracks when hidden (battery/thermal)
// Resume only active tracks when visible again
// ─────────────────────────────────────────────────────────────────
document.addEventListener("visibilitychange", () => {
  if (!localStream) return;
  if (document.hidden) {
    // Pause local video to reduce CPU / prevent overheating
    localStream.getVideoTracks().forEach(t => { t.enabled = false; });
    // Also suspend VAD while hidden
    _vadRunning = false;
  } else {
    if (camEnabled) localStream.getVideoTracks().forEach(t => { t.enabled = true; });
    // Resume VAD
    if (_vadCtx && !_vadRunning) {
      _vadRunning = true;
      // Re-kick VAD loop
      startVAD();
    }
  }
});

// ─────────────────────────────────────────────────────────────────
// Intersection Observer — pause videos scrolled out of view on mobile
// ─────────────────────────────────────────────────────────────────
if ("IntersectionObserver" in window && isMobile()) {
  const videoObserver = new IntersectionObserver(entries => {
    entries.forEach(entry => {
      const vid = entry.target;
      if (entry.isIntersecting) {
        vid.play().catch(() => {});
      } else {
        vid.pause();
      }
    });
  }, { threshold: 0.1 });

  // Observe all videos added to the grid
  const gridObserverCallback = () => {
    document.querySelectorAll(".video-box video, .mini-box video").forEach(v => {
      videoObserver.observe(v);
    });
  };
  const mo = new MutationObserver(gridObserverCallback);
  mo.observe(document.body, { childList: true, subtree: true });
}
