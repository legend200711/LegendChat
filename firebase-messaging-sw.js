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

const ICON  = '/icon-192.png';
const BADGE = '/favicon-32x32.png';

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
  const data  = payload.data  || {};
  const notif = payload.notification || {};
  const type  = data.type || 'announcement';
  const title = notif.title || TYPE_TITLES[type] || '🔔 Shadow Nexus Social';
  const body  = notif.body  || data.body || 'You have a new notification';

  return self.registration.showNotification(title, {
    body,
    icon:    ICON,
    badge:   BADGE,
    tag:     `snx-${type}-${Date.now()}`,
    renotify: true,
    vibrate: [200, 100, 200],
    data:    { url: '/' },
  });
});

// Notification click → open app
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = event.notification.data?.url || '/';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((list) => {
      for (const c of list) {
        if ('focus' in c) return c.focus();
      }
      return clients.openWindow(url);
    })
  );
});

