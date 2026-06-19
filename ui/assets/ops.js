import {
  escapeHtml,
  fetchApiJson,
  fetchCount,
  fetchOpsJson,
  fetchRows,
  formatApiTimestamp,
  formatCompact,
  formatDate,
  latestSeasonRange,
} from './supabase-rest.js';
import { mountNav, setFooter, setStatusText } from './layout.js';

mountNav('ops');
setFooter();

const dateInput = document.getElementById('ops-date-input');
const refreshButton = document.getElementById('ops-refresh');
const dateLabel = document.getElementById('ops-date-label');
const healthPill = document.getElementById('ops-health-pill');
const healthDetail = document.getElementById('ops-health-detail');
const lastChecked = document.getElementById('ops-last-checked');
const metricRoot = document.getElementById('ops-metrics');
const seasonRoot = document.getElementById('season-metrics');
const pendingList = document.getElementById('pending-list');
const pendingEmpty = document.getElementById('pending-empty');
const pendingCountPill = document.getElementById('pending-count-pill');
const commandBlock = document.getElementById('ops-command');

const season = latestSeasonRange();

function percent(value) {
  const number = Number(value || 0) * 100;
  return `${number.toFixed(number >= 10 ? 0 : 1)}%`;
}

function metricTile(label, value, detail = '') {
  return `
    <article class="metric-tile p-4">
      <p class="text-xs uppercase tracking-[0.12em] text-zinc-500">${escapeHtml(label)}</p>
      <p class="mt-2 text-3xl font-semibold leading-none">${escapeHtml(value)}</p>
      ${detail ? `<p class="mt-2 text-sm text-zinc-400">${escapeHtml(detail)}</p>` : ''}
    </article>
  `;
}

function compactRow(label, value, detail = '') {
  return `
    <div class="flex items-center justify-between gap-4 rounded-md border border-zinc-800 bg-black/30 px-3 py-2">
      <div>
        <p class="text-sm text-zinc-300">${escapeHtml(label)}</p>
        ${detail ? `<p class="text-xs text-zinc-500">${escapeHtml(detail)}</p>` : ''}
      </div>
      <p class="text-lg font-semibold text-[var(--accent-soft)]">${escapeHtml(value)}</p>
    </div>
  `;
}

function renderHealth(health) {
  const connected = health?.status === 'healthy' && health?.database === 'connected';
  healthPill.textContent = connected ? 'Healthy' : 'Needs Check';
  healthPill.classList.toggle('is-good', connected);
  healthPill.classList.toggle('is-bad', !connected);
  healthDetail.textContent = connected ? 'Database connected' : 'API issue detected';
  lastChecked.textContent = `Checked ${formatApiTimestamp(health?.timestamp)}`;
}

function renderSlate(status) {
  metricRoot.innerHTML = [
    metricTile('Slate Swords', formatCompact(status.total_swords), formatDate(status.date)),
    metricTile('Cached Videos', formatCompact(status.cached_videos), `${percent(status.cache_rate)} cache rate`),
    metricTile('Pending Videos', formatCompact(status.pending_videos), 'Missing Azure clip URL'),
    metricTile('Top Queue', formatCompact(status.top_pending?.length || 0), 'Rows returned by API'),
  ].join('');

  dateLabel.textContent = `Viewing ${formatDate(status.date)}`;
  pendingCountPill.textContent = `${status.pending_videos} pending`;
  commandBlock.textContent = `python process_daily_sword_videos.py --date ${status.date} --top-n 25`;
}

function renderSeason(total, cached) {
  const pending = Math.max(total - cached, 0);
  const rate = total ? cached / total : 0;
  seasonRoot.innerHTML = [
    compactRow('Season swords', formatCompact(total), `${season.year} regular data`),
    compactRow('Season cached', formatCompact(cached), `${percent(rate)} cache rate`),
    compactRow('Season pending', formatCompact(pending), 'Uncached sword rows'),
  ].join('');
}

function pendingTemplate(row, index) {
  const score = Number(row.sword_score || 0).toFixed(1);
  const batSpeed = row.bat_speed ? `${Number(row.bat_speed).toFixed(1)} mph` : '--';
  const miss = row.strike_zone_distance_inches !== null && row.strike_zone_distance_inches !== undefined
    ? `${Number(row.strike_zone_distance_inches).toFixed(1)} in`
    : '--';

  return `
    <div class="grid gap-3 py-3 md:grid-cols-[40px_1fr_auto] md:items-center">
      <p class="text-sm text-zinc-500">#${index + 1}</p>
      <div>
        <p class="text-lg font-semibold">
          ${escapeHtml(row.batter_name || 'Unknown hitter')}
          <span class="text-sm font-normal text-zinc-500">vs</span>
          ${escapeHtml(row.pitcher_name || row.source_player_name || 'Unknown pitcher')}
        </p>
        <p class="text-sm text-zinc-400">
          ${escapeHtml(row.pitch_name || row.pitch_type || 'Pitch')} / ${escapeHtml(row.description || 'swinging strike')}
        </p>
      </div>
      <div class="grid grid-cols-3 gap-2 text-right text-sm md:min-w-[250px]">
        <span><strong class="block text-[var(--accent-soft)]">${score}</strong><span class="text-zinc-500">score</span></span>
        <span><strong class="block text-zinc-200">${batSpeed}</strong><span class="text-zinc-500">bat</span></span>
        <span><strong class="block text-zinc-200">${miss}</strong><span class="text-zinc-500">miss</span></span>
      </div>
    </div>
  `;
}

function renderPending(rows) {
  if (!rows.length) {
    pendingList.innerHTML = '';
    pendingEmpty.classList.remove('hidden');
    return;
  }

  pendingEmpty.classList.add('hidden');
  pendingList.innerHTML = rows.map(pendingTemplate).join('');
}

async function fetchLatestDate() {
  const rows = await fetchRows('mlb_pitches_enhanced', {
    select: 'game_date',
    game_type: 'eq.R',
    sword_score: 'gte.90',
    game_date: [`gte.${season.startDate}`, `lt.${season.endDate}`],
    order: 'game_date.desc',
    limit: 1,
  });
  return rows[0]?.game_date || null;
}

async function fetchSeasonCounts() {
  const [total, cached] = await Promise.all([
    fetchCount('mlb_pitches_enhanced', {
      select: 'id',
      game_type: 'eq.R',
      sword_score: 'gte.90',
      game_date: [`gte.${season.startDate}`, `lt.${season.endDate}`],
    }),
    fetchCount('mlb_pitches_enhanced', {
      select: 'id',
      game_type: 'eq.R',
      sword_score: 'gte.90',
      video_azure_blob_url: 'not.is.null',
      game_date: [`gte.${season.startDate}`, `lt.${season.endDate}`],
    }),
  ]);
  return { total, cached };
}

async function refreshOps(date) {
  setStatusText(`Loading operations data for ${formatDate(date)}`);
  refreshButton.disabled = true;
  refreshButton.textContent = 'Loading';

  try {
    const encodedDate = encodeURIComponent(date);
    const [health, status, backlog, seasonCounts] = await Promise.all([
      fetchOpsJson('/health'),
      fetchOpsJson(`/ops/video-backlog/status?date=${encodedDate}&limit=6`),
      fetchOpsJson(`/ops/video-backlog?date=${encodedDate}&limit=12`),
      fetchSeasonCounts(),
    ]);

    renderHealth(health);
    renderSlate(status);
    renderSeason(seasonCounts.total, seasonCounts.cached);
    renderPending(backlog.pending || status.top_pending || []);
    setStatusText(`Operations current for ${formatDate(date)}.`);
  } catch (error) {
    console.error(error);
    setStatusText(`Ops load failed: ${error.message}`);
    healthPill.textContent = 'Error';
    healthPill.classList.remove('is-good');
    healthPill.classList.add('is-bad');
  } finally {
    refreshButton.disabled = false;
    refreshButton.textContent = 'Refresh';
  }
}

refreshButton.addEventListener('click', () => {
  if (dateInput.value) {
    refreshOps(dateInput.value);
  }
});

async function init() {
  try {
    const latestDate = await fetchLatestDate();
    if (!latestDate) {
      setStatusText(`No ${season.year} sword data found.`);
      return;
    }

    dateInput.value = latestDate;
    dateInput.max = latestDate;
    await refreshOps(latestDate);
  } catch (error) {
    console.error(error);
    setStatusText(`Ops load failed: ${error.message}`);
  }
}

init();

// --- Feedback inbox (admin token gated) -------------------------------------
const FEEDBACK_TOKEN_KEY = 'swordfinder:admin-token';

const feedbackTokenInput = document.getElementById('feedback-admin-token');
const feedbackStatusFilter = document.getElementById('feedback-status-filter');
const feedbackLoadButton = document.getElementById('feedback-load');
const feedbackAdminStatus = document.getElementById('feedback-admin-status');
const feedbackAdminList = document.getElementById('feedback-admin-list');
const feedbackAdminEmpty = document.getElementById('feedback-admin-empty');
const feedbackAdminPill = document.getElementById('feedback-admin-pill');

const feedbackReviewEnabled = Boolean(
  feedbackTokenInput &&
  feedbackStatusFilter &&
  feedbackLoadButton &&
  feedbackAdminStatus &&
  feedbackAdminList &&
  feedbackAdminEmpty &&
  feedbackAdminPill
);

function adminToken() {
  return (feedbackTokenInput.value || '').trim();
}

function authHeaders() {
  return { Authorization: `Bearer ${adminToken()}` };
}

function setFeedbackStatus(message, isError = false) {
  feedbackAdminStatus.textContent = message;
  feedbackAdminStatus.classList.toggle('text-red-400', isError);
}

function setAdminPill(text, good) {
  feedbackAdminPill.textContent = text;
  feedbackAdminPill.classList.toggle('is-good', Boolean(good));
  feedbackAdminPill.classList.toggle('is-bad', good === false);
}

function feedbackCardTemplate(row) {
  const created = formatApiTimestamp(row.created_at);
  const email = row.contact_email
    ? `<a class="feedback-inline-link" href="mailto:${escapeHtml(row.contact_email)}">${escapeHtml(row.contact_email)}</a>`
    : '<span class="text-zinc-600">no email</span>';
  const context = row.page_path ? escapeHtml(row.page_path) : '—';
  const reason = row.rejection_reason
    ? `<p class="mt-1 text-xs text-zinc-400">Reason: ${escapeHtml(row.rejection_reason)}</p>`
    : '';

  return `
    <div class="feedback-admin-card" data-feedback-id="${escapeHtml(String(row.id))}">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <div class="flex items-center gap-2">
          <span class="roadmap-tag roadmap-tag-${escapeHtml(row.request_type || 'feature')}">${escapeHtml(row.request_type === 'bug' ? 'Bug' : 'Feature')}</span>
          <span class="status-pill">${escapeHtml(row.status || 'new')}</span>
        </div>
        <p class="text-xs text-zinc-500">#${escapeHtml(String(row.id))} · ${escapeHtml(created)}</p>
      </div>
      <p class="mt-2 text-sm text-zinc-200">${escapeHtml(row.message || '')}</p>
      ${reason}
      <p class="mt-2 text-xs text-zinc-500">Page: ${context} · Theme: ${escapeHtml(row.theme || '—')} · ${email}</p>
      <div class="mt-3 grid gap-2 sm:grid-cols-[1fr_auto_auto_auto] sm:items-center">
        <input class="ops-date-input min-w-0 feedback-reason-input" type="text" maxlength="1000" placeholder="Rejection reason (required to reject)" />
        <button class="secondary rounded-md px-3 py-2 text-xs uppercase tracking-[0.08em]" data-feedback-action="planned">Plan</button>
        <button class="secondary rounded-md px-3 py-2 text-xs uppercase tracking-[0.08em]" data-feedback-action="shipped">Ship</button>
        <button class="secondary rounded-md px-3 py-2 text-xs uppercase tracking-[0.08em]" data-feedback-action="rejected">Reject</button>
      </div>
    </div>
  `;
}

async function updateFeedbackStatus(card, status) {
  const id = card.getAttribute('data-feedback-id');
  const reason = card.querySelector('.feedback-reason-input')?.value.trim() || '';
  if (status === 'rejected' && !reason) {
    setFeedbackStatus('A rejection reason is required to reject feedback.', true);
    card.querySelector('.feedback-reason-input')?.focus();
    return;
  }

  if (!adminToken()) {
    setFeedbackStatus('Enter the admin token first.', true);
    return;
  }

  const buttons = card.querySelectorAll('button[data-feedback-action]');
  buttons.forEach((button) => { button.disabled = true; });

  try {
    const body = { status };
    if (status === 'rejected') body.rejection_reason = reason;
    await fetchApiJson(`/feedback/${encodeURIComponent(id)}/status`, {}, {
      method: 'POST',
      headers: { ...authHeaders(), 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    setFeedbackStatus(`Feedback #${id} marked ${status}.`);
    await loadFeedback();
  } catch (error) {
    console.error(error);
    setFeedbackStatus(`Could not update #${id}: ${error.message}`, true);
    buttons.forEach((button) => { button.disabled = false; });
  }
}

function renderFeedback(rows) {
  if (!rows.length) {
    feedbackAdminList.innerHTML = '';
    feedbackAdminEmpty.classList.remove('hidden');
    return;
  }
  feedbackAdminEmpty.classList.add('hidden');
  feedbackAdminList.innerHTML = rows.map(feedbackCardTemplate).join('');
  feedbackAdminList.querySelectorAll('button[data-feedback-action]').forEach((button) => {
    button.addEventListener('click', () => {
      const card = button.closest('.feedback-admin-card');
      updateFeedbackStatus(card, button.getAttribute('data-feedback-action'));
    });
  });
}

async function loadFeedback() {
  if (!adminToken()) {
    setFeedbackStatus('Enter the admin token to load feedback.', true);
    setAdminPill('Token required', false);
    return;
  }

  feedbackLoadButton.disabled = true;
  feedbackLoadButton.textContent = 'Loading';
  setFeedbackStatus('Loading feedback…');

  try {
    const statusValue = feedbackStatusFilter.value;
    const data = await fetchApiJson('/feedback/admin', statusValue ? { status: statusValue } : {}, {
      headers: authHeaders(),
    });
    try {
      window.sessionStorage.setItem(FEEDBACK_TOKEN_KEY, adminToken());
    } catch {
      // Session storage may be blocked; loading still worked for this view.
    }
    renderFeedback(data.rows || []);
    setAdminPill('Authorized', true);
    setFeedbackStatus(`${data.count || 0} item${(data.count || 0) === 1 ? '' : 's'} loaded.`);
  } catch (error) {
    console.error(error);
    setAdminPill('Token rejected', false);
    setFeedbackStatus(`Feedback load failed: ${error.message}`, true);
  } finally {
    feedbackLoadButton.disabled = false;
    feedbackLoadButton.textContent = 'Load Feedback';
  }
}

if (feedbackReviewEnabled) {
  try {
    const savedToken = window.sessionStorage.getItem(FEEDBACK_TOKEN_KEY);
    if (savedToken) feedbackTokenInput.value = savedToken;
  } catch {
    // Ignore storage access errors.
  }

  feedbackLoadButton.addEventListener('click', loadFeedback);
  feedbackStatusFilter.addEventListener('change', () => {
    if (adminToken()) loadFeedback();
  });
  feedbackTokenInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      loadFeedback();
    }
  });
}
