from pathlib import Path


def test_feedback_launcher_module_captures_context_and_honeypot():
    source = Path("ui/assets/feedback.js").read_text()

    assert "export function mountFeedbackLauncher" in source
    # Floating launcher + modal dialog roles.
    assert 'class="feedback-launcher"' in source
    assert 'role="dialog"' in source
    assert 'aria-modal="true"' in source
    # Request type options.
    assert 'value="feature"' in source
    assert 'value="bug"' in source
    # Hidden honeypot field is submitted as `website`.
    assert 'name="website"' in source
    assert "website: websiteEl.value" in source
    # Automatic page context.
    assert "window.location.pathname" in source
    assert "window.location.href" in source
    assert "window.navigator.userAgent" in source
    assert "activeTheme()" in source
    assert "data-theme" in source
    # Posts to the feedback endpoint.
    assert "/feedback" in source
    assert "method: 'POST'" in source
    # Handles the rate-limit response distinctly.
    assert "429" in source


def test_layout_mounts_feedback_on_every_page_and_links_roadmap():
    layout = Path("ui/assets/layout.js").read_text()

    assert "import { mountFeedbackLauncher } from './feedback.js'" in layout
    assert "mountFeedbackLauncher()" in layout
    # The launcher mounts before the early return so it is shared chrome.
    assert layout.index("mountFeedbackLauncher()") < layout.index("if (!nav) return")
    # Roadmap nav link is present.
    assert 'href="/roadmap.html"' in layout
    assert "active === 'roadmap'" in layout


def test_feedback_styles_are_themeable_and_mobile_safe():
    css = Path("ui/assets/styles.css").read_text()

    assert ".feedback-launcher" in css
    assert ".feedback-modal" in css
    assert ".feedback-website" in css
    # Launcher uses safe-area insets so it does not cover mobile content.
    assert "env(safe-area-inset-bottom)" in css
    # Accent derives from the theme variables so it retints per theme.
    assert "var(--accent)" in css
    # Honeypot is moved off-screen, not display:none, so bots still fill it.
    assert "left: -10000px" in css


def test_roadmap_page_is_wired_and_shows_three_groups():
    html = Path("ui/roadmap.html").read_text()
    script = Path("ui/assets/roadmap.js").read_text()

    assert '<script type="module" src="/assets/roadmap.js"></script>' in html
    assert 'id="planned-list"' in html
    assert 'id="shipped-list"' in html
    assert 'id="rejected-list"' in html

    assert "mountNav('roadmap')" in script
    assert "/feedback/roadmap" in script
    # Rejected items must render their reason.
    assert "rejection_reason" in script
    assert "roadmap-reason" in script
    # Public roadmap never renders private contact info.
    assert "contact_email" not in script


def test_ops_ui_exposes_admin_feedback_review():
    html = Path("ui/ops.html").read_text()
    ops = Path("ui/assets/ops.js").read_text()

    assert 'id="feedback-review"' in html
    assert 'id="feedback-admin-token"' in html
    assert 'id="feedback-status-filter"' in html
    assert 'id="feedback-load"' in html

    # Uses the admin token via Authorization Bearer header.
    assert "Authorization: `Bearer ${adminToken()}`" in ops
    assert "/feedback/admin" in ops
    assert "/feedback/${encodeURIComponent(id)}/status" in ops
    # Reject path requires a reason in the UI before calling the API.
    assert "A rejection reason is required" in ops
