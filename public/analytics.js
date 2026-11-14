(() => {
  'use strict';

  const POLL_INTERVAL = 30000;
  const STORAGE_KEY = 'monopoly_tracker_seen_events';

  let checking = false;

  function escapeHTML(value) {
    return String(value).replace(/[&<>"']/g, char => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#039;'
    })[char]);
  }

  function createPanel() {
    if (document.getElementById('smartAnalytics')) return;

    const anchor = document.querySelector('header');

    if (!anchor) return;

    const section = document.createElement('section');

    section.className = 'card';
    section.id = 'smartAnalytics';
    section.style.marginTop = '15px';

    section.innerHTML = `
      <h2 class="section-title">🧠 Smart Analytics</h2>

      <div style="margin-bottom:15px">
        <button id="enableNotifications">
          🔔 Enable Notifications
        </button>
        <span id="notificationStatus"
          style="font-size:12px;color:#9caac5">
          Notifications not enabled
        </span>
      </div>

      <div id="analyticsSummary">
        Loading analytics...
      </div>

      <h3 style="margin-top:20px">
        Historical Event Intervals
      </h3>

      <div id="intervalStats">
        Collecting historical records...
      </div>

      <p style="
        color:#8e9bb4;
        font-size:12px;
        line-height:1.5;
      ">
        Analytics are based on recorded historical events.
        They do not predict or guarantee future game outcomes.
      </p>
    `;

    anchor.after(section);

    document
      .getElementById('enableNotifications')
      .addEventListener('click', enableNotifications);
  }

  async function enableNotifications() {
    const status = document.getElementById('notificationStatus');

    if (!('Notification' in window)) {
      status.textContent = 'Notifications are not supported.';
      return;
    }

    const permission = await Notification.requestPermission();

    status.textContent =
      permission === 'granted'
        ? 'Notifications enabled'
        : 'Notifications permission not granted';
  }

  function getSeenEvents() {
    try {
      return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
    } catch {
      return [];
    }
  }

  function saveSeenEvents(events) {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify(events.slice(-500))
    );
  }

  function notifyEvent(record) {
    if (!('Notification' in window)) return;
    if (Notification.permission !== 'granted') return;

    let title = '';
    let message = '';

    if (record.is_three_rolls) {
      title = '🎯 MONOPOLY - 3 Rolls';
      message = 'A new 3 Rolls event was recorded.';
    } else if (record.is_five_rolls) {
      title = '🎯 MONOPOLY - 5 Rolls';
      message = 'A new 5 Rolls event was recorded.';
    } else if (record.is_big_win) {
      title = '💰 MONOPOLY - BIG WIN';
      message =
        `Big Win recorded: ${record.big_win_multiplier || '100+'}x`;
    }

    if (!title) return;

    try {
      new Notification(title, {
        body: message,
        tag: String(record.event_id)
      });
    } catch (error) {
      console.error('Notification error:', error);
    }
  }

  async function checkNewEvents() {
    if (checking) return;

    checking = true;

    try {
      const response = await fetch('/api/history?type=all&limit=100');

      if (!response.ok) {
        throw new Error('Unable to retrieve history');
      }

      const data = await response.json();
      const records = data.records || [];

      const seen = new Set(getSeenEvents());
      const newSeen = new Set(seen);

      for (const record of records) {
        const id = String(record.event_id);

        if (!id || seen.has(id)) continue;

        // First run: mark existing records as seen.
        if (seen.size === 0) {
          newSeen.add(id);
          continue;
        }

        notifyEvent(record);
        newSeen.add(id);
      }

      saveSeenEvents([...newSeen]);

      updateAnalytics(records);

    } catch (error) {
      console.error('Analytics error:', error);
    } finally {
      checking = false;
    }
  }

  function updateAnalytics(records) {
    const summary = document.getElementById('analyticsSummary');
    const intervalBox = document.getElementById('intervalStats');

    if (!summary || !intervalBox) return;

    const three = records
      .filter(r => r.is_three_rolls)
      .sort((a, b) =>
        new Date(b.settled_at) - new Date(a.settled_at)
      );

    const five = records
      .filter(r => r.is_five_rolls)
      .sort((a, b) =>
        new Date(b.settled_at) - new Date(a.settled_at)
      );

    const bigWins = records.filter(r => r.is_big_win);

    summary.innerHTML = `
      <div style="
        display:grid;
        grid-template-columns:repeat(3,1fr);
        gap:8px;
      ">
        <div>
          <div style="color:#9caac5;font-size:12px">3 Rolls</div>
          <strong style="color:#35d48a;font-size:22px">
            ${three.length}
          </strong>
        </div>

        <div>
          <div style="color:#9caac5;font-size:12px">5 Rolls</div>
          <strong style="color:#f5c542;font-size:22px">
            ${five.length}
          </strong>
        </div>

        <div>
          <div style="color:#9caac5;font-size:12px">Big Wins</div>
          <strong style="color:#ff6464;font-size:22px">
            ${bigWins.length}
          </strong>
        </div>
      </div>
    `;

    function calculateIntervals(list) {
      if (list.length < 2) return null;

      const intervals = [];

      for (let i = 0; i < list.length - 1; i++) {
        const newer = new Date(list[i].settled_at).getTime();
        const older = new Date(list[i + 1].settled_at).getTime();

        const minutes = (newer - older) / 60000;

        if (Number.isFinite(minutes) && minutes >= 0) {
          intervals.push(minutes);
        }
      }

      if (!intervals.length) return null;

      const average =
        intervals.reduce((a, b) => a + b, 0) /
        intervals.length;

      return {
        average,
        shortest: Math.min(...intervals),
        longest: Math.max(...intervals),
        samples: intervals.length
      };
    }

    function intervalHTML(name, list) {
      const stats = calculateIntervals(list);

      if (!stats) {
        return `
          <div style="padding:10px 0">
            <strong>${name}</strong><br>
            <span style="color:#9caac5">
              Need more historical records.
            </span>
          </div>
        `;
      }

      return `
        <div style="
          padding:12px 0;
          border-bottom:1px solid #293653;
        ">
          <strong>${name}</strong>

          <div style="
            display:grid;
            grid-template-columns:repeat(3,1fr);
            gap:5px;
            margin-top:10px;
          ">
            <div>
              <small style="color:#9caac5">Average</small><br>
              <strong>${stats.average.toFixed(1)} min</strong>
            </div>

            <div>
              <small style="color:#9caac5">Shortest</small><br>
              <strong>${stats.shortest.toFixed(1)} min</strong>
            </div>

            <div>
              <small style="color:#9caac5">Longest</small><br>
              <strong>${stats.longest.toFixed(1)} min</strong>
            </div>
          </div>

          <small style="color:#8e9bb4">
            Based on ${stats.samples} historical intervals.
          </small>
        </div>
      `;
    }

    intervalBox.innerHTML =
      intervalHTML('🎯 3 Rolls', three) +
      intervalHTML('🎯 5 Rolls', five);
  }

  function initialize() {
    createPanel();

    const permissionStatus =
      document.getElementById('notificationStatus');

    if (
      permissionStatus &&
      'Notification' in window &&
      Notification.permission === 'granted'
    ) {
      permissionStatus.textContent = 'Notifications enabled';
    }

    checkNewEvents();

    setInterval(checkNewEvents, POLL_INTERVAL);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initialize);
  } else {
    initialize();
  }

})();
