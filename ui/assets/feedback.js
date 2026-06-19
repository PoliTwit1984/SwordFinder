// Floating in-app Feedback launcher + modal.
//
// Mounted on every page by layout.mountNav(). The launcher opens a themed modal
// with a request type (feature request / bug report), a message, an optional
// contact email, and a hidden honeypot field. Page context (path, url, user
// agent, active theme) is captured automatically and POSTed to the API.
//
// Kept dependency-free (reads window.SWORDFINDER_CONFIG and the data-theme
// attribute directly) so it can be imported from layout.js without import cycles.

const FEEDBACK_ENDPOINT = '/feedback';
const MAX_MESSAGE_LENGTH = 2000;

let launcherMounted = false;

function apiBaseUrl() {
  const cfg = window.SWORDFINDER_CONFIG || {};
  return (cfg.apiBaseUrl || '').replace(/\/$/, '');
}

function activeTheme() {
  return document.documentElement.getAttribute('data-theme') || '';
}

export function mountFeedbackLauncher() {
  if (launcherMounted) return;
  if (typeof document === 'undefined' || !document.body) return;
  launcherMounted = true;

  const root = document.createElement('div');
  root.className = 'feedback-root';
  root.innerHTML = `
    <button type="button" class="feedback-launcher" id="feedback-launcher" aria-haspopup="dialog" aria-controls="feedback-modal" aria-expanded="false">
      <span class="feedback-launcher-icon" aria-hidden="true">✦</span>
      <span class="feedback-launcher-label">Feedback</span>
    </button>
    <div class="feedback-modal" id="feedback-modal" role="dialog" aria-modal="true" aria-labelledby="feedback-modal-title" hidden>
      <div class="feedback-panel">
        <button type="button" class="feedback-close" data-feedback-dismiss aria-label="Close feedback">&times;</button>
        <p class="text-xs uppercase tracking-[0.14em] text-zinc-500">Help shape SwordFinder</p>
        <h2 id="feedback-modal-title" class="brand-title mt-2 text-3xl leading-none text-zinc-100">Send Feedback</h2>
        <p class="mt-2 text-sm text-zinc-400">Request a feature or report a bug. Track what's coming on the <a class="feedback-inline-link" href="/roadmap.html">public roadmap</a>.</p>
        <form class="feedback-form" id="feedback-form" novalidate>
          <label class="feedback-field">
            <span>Type</span>
            <select name="request_type" id="feedback-type" class="ops-date-input">
              <option value="feature">Feature request</option>
              <option value="bug">Bug report</option>
            </select>
          </label>
          <label class="feedback-field">
            <span>Message</span>
            <textarea name="message" id="feedback-message" class="ops-date-input" rows="4" maxlength="${MAX_MESSAGE_LENGTH}" required placeholder="What would make SwordFinder better?"></textarea>
          </label>
          <label class="feedback-field">
            <span>Contact email <em>(optional)</em></span>
            <input name="contact_email" id="feedback-email" class="ops-date-input" type="email" maxlength="254" placeholder="you@example.com" autocomplete="email" />
          </label>
          <div class="feedback-website" aria-hidden="true">
            <label>Leave this field empty
              <input type="text" name="website" id="feedback-website" tabindex="-1" autocomplete="off" />
            </label>
          </div>
          <p class="feedback-status" id="feedback-status" role="status" aria-live="polite"></p>
          <div class="feedback-actions">
            <button type="button" class="secondary rounded-md px-4 py-2 text-sm uppercase tracking-[0.08em]" data-feedback-dismiss>Cancel</button>
            <button type="submit" class="primary rounded-md px-4 py-2 text-sm uppercase tracking-[0.08em]" id="feedback-submit">Send</button>
          </div>
        </form>
      </div>
    </div>
  `;
  document.body.appendChild(root);

  const launcher = root.querySelector('#feedback-launcher');
  const modal = root.querySelector('#feedback-modal');
  const form = root.querySelector('#feedback-form');
  const typeEl = root.querySelector('#feedback-type');
  const messageEl = root.querySelector('#feedback-message');
  const emailEl = root.querySelector('#feedback-email');
  const websiteEl = root.querySelector('#feedback-website');
  const statusEl = root.querySelector('#feedback-status');
  const submitEl = root.querySelector('#feedback-submit');

  let lastFocused = null;

  function openModal() {
    lastFocused = document.activeElement;
    modal.hidden = false;
    // Allow the [hidden] removal to paint before transitioning in.
    window.requestAnimationFrame(() => modal.classList.add('is-visible'));
    launcher.setAttribute('aria-expanded', 'true');
    statusEl.textContent = '';
    statusEl.classList.remove('is-error', 'is-success');
    window.setTimeout(() => messageEl.focus(), 60);
  }

  function closeModal() {
    modal.classList.remove('is-visible');
    launcher.setAttribute('aria-expanded', 'false');
    window.setTimeout(() => {
      modal.hidden = true;
    }, 180);
    if (lastFocused && typeof lastFocused.focus === 'function') {
      lastFocused.focus();
    } else {
      launcher.focus();
    }
  }

  async function submitFeedback() {
    const message = messageEl.value.trim();
    statusEl.classList.remove('is-error', 'is-success');

    if (!message) {
      statusEl.textContent = 'Please enter a message.';
      statusEl.classList.add('is-error');
      messageEl.focus();
      return;
    }

    const base = apiBaseUrl();
    if (!base) {
      statusEl.textContent = 'Feedback is not configured right now.';
      statusEl.classList.add('is-error');
      return;
    }

    const payload = {
      request_type: typeEl.value,
      message,
      contact_email: emailEl.value.trim() || null,
      website: websiteEl.value, // honeypot
      page_path: window.location.pathname,
      page_url: window.location.href,
      user_agent: window.navigator.userAgent,
      theme: activeTheme(),
    };

    submitEl.disabled = true;
    submitEl.textContent = 'Sending';
    statusEl.textContent = '';

    try {
      const response = await fetch(`${base}${FEEDBACK_ENDPOINT}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        credentials: 'include',
      });

      if (!response.ok) {
        let detail = `Request failed (${response.status})`;
        if (response.status === 429) {
          detail = 'You are sending feedback too quickly. Please try again in a few minutes.';
        } else {
          try {
            const body = await response.json();
            if (body && body.detail) detail = body.detail;
          } catch {
            // Non-JSON error body; keep the generic message.
          }
        }
        throw new Error(detail);
      }

      statusEl.textContent = 'Thanks! Your feedback was sent.';
      statusEl.classList.add('is-success');
      form.reset();
      window.setTimeout(closeModal, 1200);
    } catch (error) {
      statusEl.textContent = error.message || 'Could not send feedback right now.';
      statusEl.classList.add('is-error');
    } finally {
      submitEl.disabled = false;
      submitEl.textContent = 'Send';
    }
  }

  launcher.addEventListener('click', openModal);
  form.addEventListener('submit', (event) => {
    event.preventDefault();
    submitFeedback();
  });
  root.querySelectorAll('[data-feedback-dismiss]').forEach((target) => {
    target.addEventListener('click', closeModal);
  });
  modal.addEventListener('click', (event) => {
    if (event.target === modal) closeModal();
  });
  modal.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeModal();
  });
}
