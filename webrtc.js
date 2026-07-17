/**
 * Shadow Nexus Social — webrtc.js
 *
 * WebRTC helpers for the live streaming feature.
 * Exports two functions: hostAcceptViewer() and viewerConnect().
 * Both are thin wrappers around RTCPeerConnection that delegate
 * all Firebase signalling to the LiveDB object (firebase-live.js).
 */

/* ── Public STUN servers (cover most NAT types) ── */
export const ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302'  },
  { urls: 'stun:stun1.l.google.com:19302' },
  { urls: 'stun:stun2.l.google.com:19302' },
  { urls: 'stun:stun.cloudflare.com:3478' },
];

/* ══════════════════════════════════════════════════════════
   HOST SIDE
   ── Called once per viewer offer.
   ── Creates a peer connection, sends answer, relays ICE.
   ══════════════════════════════════════════════════════════ */

/**
 * @param {object}      opts
 * @param {MediaStream} opts.stream       Local camera+mic stream
 * @param {string}      opts.roomId       Active room ID
 * @param {string}      opts.viewerKey    Viewer's UID (offer key)
 * @param {object}      opts.offer        { sdp: string }
 * @param {object}      opts.db           LiveDB instance
 * @returns {RTCPeerConnection}
 */
export async function hostAcceptViewer({ stream, roomId, viewerKey, offer, db }) {
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

  /* 1. Add local media tracks to the connection */
  stream.getTracks().forEach(track => pc.addTrack(track, stream));

  /* 2. Send host ICE candidates to RTDB so the viewer can reach us */
  pc.onicecandidate = async e => {
    if (!e.candidate) return;
    await db.pushHostIce(roomId, viewerKey, e.candidate.toJSON());
  };

  /* 3. Set the viewer's offer as the remote description */
  await pc.setRemoteDescription(
    new RTCSessionDescription({ type: 'offer', sdp: offer.sdp })
  );

  /* 4. Create and send the answer */
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);
  await db.pushAnswer(roomId, viewerKey, answer.sdp);

  /* 5. Apply incoming viewer ICE candidates */
  db.subscribeViewerIce(roomId, viewerKey, candidate => {
    try { pc.addIceCandidate(new RTCIceCandidate(candidate)); } catch (_) {}
  });

  return pc;
}

/* ══════════════════════════════════════════════════════════
   VIEWER SIDE
   ── Creates an offer, waits for the host answer, relays ICE.
   ── Returns the RTCPeerConnection; caller attaches ontrack.
   ══════════════════════════════════════════════════════════ */

/**
 * @param {object}   opts
 * @param {string}   opts.roomId     Room to join
 * @param {string}   opts.viewerUid  Viewer's own UID (used as key)
 * @param {object}   opts.db         LiveDB instance
 * @param {Function} opts.onStream   Called with the remote MediaStream
 * @returns {RTCPeerConnection}
 */
export async function viewerConnect({ roomId, viewerUid, db, onStream }) {
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

  /* 1. Collect viewer ICE candidates and forward to host */
  pc.onicecandidate = async e => {
    if (!e.candidate) return;
    await db.pushViewerIce(roomId, viewerUid, e.candidate.toJSON());
  };

  /* 2. Receive host media stream */
  pc.ontrack = e => {
    if (e.streams?.[0] && typeof onStream === 'function') {
      onStream(e.streams[0]);
    }
  };

  /* 3. Create offer and write to RTDB */
  const offer = await pc.createOffer({ offerToReceiveVideo: true, offerToReceiveAudio: true });
  await pc.setLocalDescription(offer);
  await db.pushOffer(roomId, viewerUid, offer.sdp);

  /* 4. Wait for host answer */
  let answerApplied = false;
  db.subscribeAnswer(roomId, viewerUid, async ans => {
    if (answerApplied || !ans?.sdp) return;
    answerApplied = true;
    try {
      await pc.setRemoteDescription(
        new RTCSessionDescription({ type: 'answer', sdp: ans.sdp })
      );
    } catch (_) {}
  });

  /* 5. Apply host ICE candidates */
  db.subscribeHostIce(roomId, viewerUid, candidate => {
    try { pc.addIceCandidate(new RTCIceCandidate(candidate)); } catch (_) {}
  });

  return pc;
}
