/**
 * Shadow Nexus Social — Firebase Messaging Service Worker
 * Handles background push notifications when the app is closed/locked.
 * Uses Firestore real-time listener to show OS notifications.
 */

importScripts('https://www.gstatic.com/firebasejs/10.8.0/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.8.0/firebase-messaging-compat.js');

firebase.initializeApp({
  apiKey:            'AIzaSyByZRmp6R9HY17T2_WdJUFWeeaLNOP6y2Y',
  authDomain:        'horr-a08f4.firebaseapp.com',
  projectId:         'horr-a08f4',
  storageBucket:     'horr-a08f4.firebasestorage.app',
  messagingSenderId: '933810617818',
  appId:             '1:933810617818:web:efb24f123337dd987c14e3',
});

const messaging = firebase.messaging();

// GitHub Pages serves this app under /ShadowNexusSocial/
// Use absolute paths so icons resolve correctly regardless of SW scope.
const SNX_BASE = '/ShadowNexusSocial/';
const ICON  = SNX_BASE + 'icon-192.png';
const BADGE = SNX_BASE + 'favicon-32x32.png';
const APP_URL = SNX_BASE;

const TYPE_TITLES = {
  message:       '💬 New Message',
  like:          '❤️ Post Liked',
  comment:       '💬 New Comment',
  follow:        '👤 New Follower',
  friendRequest: '🦋 Friend Request',
  mention:       '@ You were mentioned',
  announcement:  '📢 Announcement',
  repost:        '🔄 Post Reposted',
  wallPost:      '📝 New Wall Post',
};

// Background FCM messages (app closed / background)
messaging.onBackgroundMessage((payload) => {
  const data    = payload.data  || {};
  const notif   = payload.notification || {};
  const type    = data.type    || 'announcement';
  const fromUid = data.fromUid || '';
  const title   = notif.title || TYPE_TITLES[type] || '🔔 Shadow Nexus Social';
  const body    = notif.body  || data.body || 'You have a new notification';

  return self.registration.showNotification(title, {
    body,
    icon:    ICON,
    badge:   BADGE,
    tag:     `snx-${type}-${Date.now()}`,
    renotify: true,
    vibrate: [200, 100, 200],
    data:    { url: APP_URL, type, fromUid },
  });
});

// Notification click → for message notifications open the Profile Message modal;
// for everything else focus the existing tab or open the app.
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const data    = event.notification.data || {};
  const url     = data.url || APP_URL;
  const type    = data.type    || '';
  const fromUid = data.fromUid || '';

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((list) => {
      const appTab = list.find(c => c.url.includes('/ShadowNexusSocial'));

      if (type === 'message' && fromUid) {
        // Route message taps: tell the open page to call ipcOpen(fromUid)
        if (appTab) {
          appTab.focus();
          appTab.postMessage({ type: 'SNX_OPEN_CHAT', fromUid });
          return;
        }
        // App not open — open it; page will handle SNX_OPEN_CHAT on next load via sessionStorage
        return clients.openWindow(url + '?snxChat=' + encodeURIComponent(fromUid));
      }

      // Default: focus existing tab or open app
      if (appTab && 'focus' in appTab) return appTab.focus();
      return clients.openWindow(url);
    })
  );
});

