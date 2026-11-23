// ========================================
// BIG BALLER LIVE TRACKER - SERVICE WORKER
// ========================================

const APP_VERSION = '1.0.1';
const CACHE_NAME = `big-baller-tracker-v${APP_VERSION}`;

const APP_FILES = [
  '/',
  '/manifest.json',
  '/icon-192.png',
  '/icon-512.png'
];

// INSTALL
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(APP_FILES))
  );
});

// ACTIVATE UPDATE WHEN USER ACCEPTS
self.addEventListener('message', event => {
  if (event.data === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

// ACTIVATE
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_NAME)
          .map(key => caches.delete(key))
      )
    ).then(() => self.clients.claim())
  );
});

// FETCH
self.addEventListener('fetch', event => {
  const request = event.request;
  const url = new URL(request.url);

  // Huwag i-cache ang API requests o external websites.
  if (
    request.method !== 'GET' ||
    url.origin !== self.location.origin ||
    url.pathname.startsWith('/api/')
  ) {
    return;
  }

  // Network-first para sa main website.
  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request)
        .then(response => {
          if (response.ok) {
            const copy = response.clone();

            caches.open(CACHE_NAME)
              .then(cache => cache.put('/', copy));
          }

          return response;
        })
        .catch(async () => {
          const cached = await caches.match('/');

          if (cached) {
            return cached;
          }

          return new Response(
            'Offline ka ngayon. Kumonekta sa internet at subukan ulit.',
            {
              status: 503,
              headers: {
                'Content-Type': 'text/plain; charset=utf-8'
              }
            }
          );
        })
    );

    return;
  }

  // Cache-first para sa app files.
  if (
    url.pathname === '/manifest.json' ||
    url.pathname === '/icon-192.png' ||
    url.pathname === '/icon-512.png'
  ) {
    event.respondWith(
      caches.match(request).then(cached => {
        return cached || fetch(request);
      })
    );
  }
});
