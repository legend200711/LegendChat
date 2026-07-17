/**
 * Shadow Nexus Social — firebase-live.js
 *
 * Firebase Realtime Database helpers for the live streaming feature.
 * Exports a single LiveDB object used by live.js.
 *
 * Data layout in RTDB:
 *   liveRooms/
 *     {roomId}/
 *       host/       { uid, username, avatar, isLive, thumbnail }
 *       startedAt   timestamp
 *       viewerCount number
 *       likes/count number
 *       viewers/    { uid: { username, joinedAt } }
 *       messages/   { pushId: { username, message, timestamp } }
 *       offers/     { viewerUid: { sdp, answered } }
 *       answers/    { viewerUid: { sdp, type } }
 *       viewerCandidates/ { viewerUid/pushId: ICECandidate }
 *       hostCandidates/   { viewerUid/pushId: ICECandidate }
 */

import { initializeApp, getApps, getApp } from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js';
import {
  getDatabase, ref, set, push, get, update, remove,
  onValue, onDisconnect, off, serverTimestamp,
} from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js';

/* ── Firebase config (same project as index.html) ── */
const _CFG = {
  apiKey:            'AIzaSyByZRmp6R9HY17T2_WdJUFWeeaLNOP6y2Y',
  authDomain:        'horr-a08f4.firebaseapp.com',
  databaseURL:       'https://horr-a08f4-default-rtdb.firebaseio.com',
  projectId:         'horr-a08f4',
  storageBucket:     'horr-a08f4.firebasestorage.app',
  messagingSenderId: '933810617818',
  appId:             '1:933810617818:web:efb24f123337dd987c14e3',
};

/* Re-use the app if already initialised (e.g. when loaded alongside index.html).
   When running as a standalone page: init the default app with our config.
   When loaded inside the main app:   re-use the existing default app.         */
let _app;
if (getApps().length) {
  /* Try to find our named app first, fall back to default */
  _app = getApps().find(a => a.name === 'snx-live') || getApp();
} else {
  _app = initializeApp(_CFG);
}
const _db  = getDatabase(_app);

/* ══════════════════════════════════════════════════════════
   LiveDB — all RTDB operations for the live module
   ══════════════════════════════════════════════════════════ */
export const LiveDB = {

  /* ── Create a new live room and return its ID ── */
  async createRoom({ uid, username, avatar }) {
    const roomRef = push(ref(_db, 'liveRooms'));
    await set(roomRef, {
      host:        { uid, username, avatar, isLive: true, thumbnail: '' },
      startedAt:   Date.now(),
      viewerCount: 0,
      likes:       { count: 0 },
    });
    /* Auto-clean when host disconnects */
    onDisconnect(ref(_db, `liveRooms/${roomRef.key}/host/isLive`)).set(false);
    onDisconnect(ref(_db, `liveRooms/${roomRef.key}/viewerCount`)).set(0);
    return roomRef.key;
  },

  /* ── Mark the room as ended ── */
  async endRoom(roomId) {
    await update(ref(_db, `liveRooms/${roomId}/host`), { isLive: false });
  },

  /* ── Store R2 thumbnail URL ── */
  async setThumbnail(roomId, url) {
    await update(ref(_db, `liveRooms/${roomId}/host`), { thumbnail: url });
  },

  /* ── Get all active rooms (isLive === true) ── */
  async getActiveRooms() {
    const snap = await get(ref(_db, 'liveRooms'));
    if (!snap.exists()) return [];
    const rooms = [];
    snap.forEach(child => {
      const d = child.val();
      if (d?.host?.isLive) rooms.push({ id: child.key, ...d });
    });
    return rooms;
  },

  /* ── Get a single room's host data ── */
  async getRoomHost(roomId) {
    const snap = await get(ref(_db, `liveRooms/${roomId}/host`));
    return snap.exists() ? snap.val() : null;
  },

  /* ── Viewer: add / remove presence + viewer count ── */
  async viewerJoin(roomId, uid, username) {
    const vRef = ref(_db, `liveRooms/${roomId}/viewers/${uid}`);
    await set(vRef, { username, joinedAt: Date.now() });
    onDisconnect(vRef).remove();

    const vcRef = ref(_db, `liveRooms/${roomId}/viewerCount`);
    const snap  = await get(vcRef);
    const count = (snap.val() || 0) + 1;
    await set(vcRef, count);
    onDisconnect(vcRef).set(Math.max(0, count - 1));
  },

  async viewerLeave(roomId, uid) {
    await remove(ref(_db, `liveRooms/${roomId}/viewers/${uid}`));
    const vcRef = ref(_db, `liveRooms/${roomId}/viewerCount`);
    const snap  = await get(vcRef);
    await set(vcRef, Math.max(0, (snap.val() || 1) - 1));
    /* Clean up signalling data */
    await remove(ref(_db, `liveRooms/${roomId}/offers/${uid}`));
    await remove(ref(_db, `liveRooms/${roomId}/viewerCandidates/${uid}`));
  },

  /* ── Comments ── */
  async pushComment(roomId, username, message) {
    await push(ref(_db, `liveRooms/${roomId}/messages`), {
      username, message, timestamp: Date.now(),
    });
  },

  subscribeComments(roomId, callback) {
    const r = ref(_db, `liveRooms/${roomId}/messages`);
    onValue(r, snap => {
      const msgs = [];
      snap.forEach(c => msgs.push(c.val()));
      callback(msgs);
    });
    return r;
  },

  /* ── Likes ── */
  async incrementLikes(roomId) {
    const r = ref(_db, `liveRooms/${roomId}/likes/count`);
    const snap = await get(r);
    await set(r, (snap.val() || 0) + 1);
  },

  subscribeLikes(roomId, callback) {
    const r = ref(_db, `liveRooms/${roomId}/likes/count`);
    onValue(r, snap => callback(snap.val() || 0));
    return r;
  },

  /* ── Viewer count ── */
  subscribeViewers(roomId, callback) {
    const r = ref(_db, `liveRooms/${roomId}/viewerCount`);
    onValue(r, snap => callback(snap.val() || 0));
    return r;
  },

  /* ── WebRTC signalling ── */
  async pushOffer(roomId, viewerUid, sdp) {
    await set(ref(_db, `liveRooms/${roomId}/offers/${viewerUid}`), { sdp, answered: false });
  },

  async pushAnswer(roomId, viewerUid, sdp) {
    await set(ref(_db, `liveRooms/${roomId}/answers/${viewerUid}`), { sdp, type: 'answer' });
    await update(ref(_db, `liveRooms/${roomId}/offers/${viewerUid}`), { answered: true });
  },

  async pushHostIce(roomId, viewerUid, candidate) {
    await push(ref(_db, `liveRooms/${roomId}/hostCandidates/${viewerUid}`), candidate);
  },

  async pushViewerIce(roomId, viewerUid, candidate) {
    await push(ref(_db, `liveRooms/${roomId}/viewerCandidates/${viewerUid}`), candidate);
  },

  subscribeOffers(roomId, callback) {
    const r = ref(_db, `liveRooms/${roomId}/offers`);
    onValue(r, snap => {
      snap.forEach(child => {
        const v = child.val();
        if (v?.sdp && !v.answered) callback(child.key, v);
      });
    });
    return r;
  },

  subscribeAnswer(roomId, viewerUid, callback) {
    const r = ref(_db, `liveRooms/${roomId}/answers/${viewerUid}`);
    onValue(r, snap => { if (snap.exists()) callback(snap.val()); });
    return r;
  },

  subscribeHostIce(roomId, viewerUid, callback) {
    const r = ref(_db, `liveRooms/${roomId}/hostCandidates/${viewerUid}`);
    onValue(r, snap => { snap.forEach(c => callback(c.val())); });
    return r;
  },

  subscribeViewerIce(roomId, viewerUid, callback) {
    const r = ref(_db, `liveRooms/${roomId}/viewerCandidates/${viewerUid}`);
    onValue(r, snap => { snap.forEach(c => callback(c.val())); });
    return r;
  },

  /* ── Unsubscribe a ref ── */
  unsub(r) { try { off(r); } catch(_) {} },
};
