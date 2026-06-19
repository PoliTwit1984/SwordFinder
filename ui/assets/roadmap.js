import {
  escapeHtml,
  fetchApiJson,
  formatApiTimestamp,
} from './supabase-rest.js';
import { mountNav, setFooter, setStatusText } from './layout.js';

mountNav('roadmap');
setFooter();

const GROUPS = ['planned', 'shipped', 'rejected'];

const lists = {
  planned: document.getElementById('planned-list'),
  shipped: document.getElementById('shipped-list'),
  rejected: document.getElementById('rejected-list'),
};
const empties = {
  planned: document.getElementById('planned-empty'),
  shipped: document.getElementById('shipped-empty'),
  rejected: document.getElementById('rejected-empty'),
};
const counts = {
  planned: document.getElementById('planned-count'),
  shipped: document.getElementById('shipped-count'),
  rejected: document.getElementById('rejected-count'),
};

function typeLabel(requestType) {
  return requestType === 'bug' ? 'Bug' : 'Feature';
}

function itemTemplate(item) {
  const title = item.title || 'Untitled request';
  const detail = item.message && item.message !== item.title ? item.message : '';
  // Rejected items must surface the reason publicly.
  const reason = item.status === 'rejected' && item.rejection_reason
    ? `<p class="roadmap-reason"><span>Why we passed:</span> ${escapeHtml(item.rejection_reason)}</p>`
    : '';

  return `
    <div class="roadmap-item">
      <div class="flex items-start justify-between gap-3">
        <p class="roadmap-item-title">${escapeHtml(title)}</p>
        <span class="roadmap-tag roadmap-tag-${escapeHtml(item.request_type || 'feature')}">${escapeHtml(typeLabel(item.request_type))}</span>
      </div>
      ${detail ? `<p class="roadmap-item-detail">${escapeHtml(detail)}</p>` : ''}
      ${reason}
      <p class="roadmap-item-meta">Updated ${escapeHtml(formatApiTimestamp(item.updated_at))}</p>
    </div>
  `;
}

function renderGroup(name, items) {
  const list = lists[name];
  const empty = empties[name];
  const count = counts[name];
  if (!list || !empty || !count) return;

  count.textContent = String(items.length);
  if (!items.length) {
    list.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }

  empty.classList.add('hidden');
  list.innerHTML = items.map(itemTemplate).join('');
}

async function init() {
  try {
    setStatusText('Loading roadmap');
    const data = await fetchApiJson('/feedback/roadmap');
    GROUPS.forEach((name) => renderGroup(name, data[name] || []));
    const total = GROUPS.reduce((sum, name) => sum + (data[name]?.length || 0), 0);
    setStatusText(total ? `${total} roadmap items.` : 'No roadmap items yet. Be the first to send feedback.');
  } catch (error) {
    console.error(error);
    setStatusText(`Could not load the roadmap: ${error.message}`);
    GROUPS.forEach((name) => renderGroup(name, []));
  }
}

init();
