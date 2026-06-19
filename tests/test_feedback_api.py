import pytest
from fastapi import HTTPException

from api_services import feedback as fb


# --- Submission validation ---------------------------------------------------
def test_normalize_request_type_accepts_aliases_and_rejects_unknown():
    assert fb.normalize_request_type("feature") == "feature"
    assert fb.normalize_request_type("Feature Request") == "feature"
    assert fb.normalize_request_type("bug_report") == "bug"
    assert fb.normalize_request_type("ISSUE") == "bug"

    with pytest.raises(HTTPException) as exc:
        fb.normalize_request_type("complaint")
    assert exc.value.status_code == 400


def test_validate_message_requires_text_and_caps_length():
    assert fb.validate_message("  add dark mode  ") == "add dark mode"

    with pytest.raises(HTTPException) as empty:
        fb.validate_message("   ")
    assert empty.value.status_code == 400

    with pytest.raises(HTTPException) as too_long:
        fb.validate_message("x" * (fb.MAX_MESSAGE_LENGTH + 1))
    assert too_long.value.status_code == 400


def test_validate_contact_email_is_optional_but_must_be_valid():
    assert fb.validate_contact_email(None) is None
    assert fb.validate_contact_email("") is None
    assert fb.validate_contact_email("Fan@Example.COM") == "fan@example.com"

    with pytest.raises(HTTPException) as exc:
        fb.validate_contact_email("not-an-email")
    assert exc.value.status_code == 400


def test_clean_context_trims_and_caps_without_raising():
    assert fb.clean_context(None) is None
    assert fb.clean_context("   ") is None
    assert fb.clean_context("/leaderboards.html") == "/leaderboards.html"
    long_agent = "Mozilla/5.0 " + "x" * 1000
    assert len(fb.clean_context(long_agent)) == fb.MAX_CONTEXT_LENGTH
    assert len(fb.clean_context("red-theme", fb.MAX_THEME_LENGTH)) <= fb.MAX_THEME_LENGTH


def test_build_feedback_record_normalizes_and_defaults_to_new():
    record = fb.build_feedback_record(
        fb.FeedbackSubmission(
            request_type="Feature Request",
            message="  please add splits  ",
            contact_email="Fan@Example.com",
            page_path="/index.html",
            page_url="https://swordfinder.com/index.html",
            user_agent="Mozilla/5.0",
            theme="blue",
        )
    )

    assert record["request_type"] == "feature"
    assert record["message"] == "please add splits"
    assert record["contact_email"] == "fan@example.com"
    assert record["status"] == fb.STATUS_NEW
    assert record["theme"] == "blue"


# --- Honeypot ----------------------------------------------------------------
def test_honeypot_detects_filled_decoy_field():
    assert fb.is_honeypot_triggered(fb.FeedbackSubmission(message="hi")) is False
    assert fb.is_honeypot_triggered(fb.FeedbackSubmission(message="hi", website="")) is False
    assert fb.is_honeypot_triggered(fb.FeedbackSubmission(message="hi", website="spam")) is True


# --- Status update validation ------------------------------------------------
def test_build_status_update_requires_reason_for_rejection():
    changes = fb.build_status_update(
        fb.FeedbackStatusUpdate(status="rejected", rejection_reason="Out of scope")
    )
    assert changes["status"] == "rejected"
    assert changes["rejection_reason"] == "Out of scope"

    with pytest.raises(HTTPException) as exc:
        fb.build_status_update(fb.FeedbackStatusUpdate(status="rejected"))
    assert exc.value.status_code == 400


def test_build_status_update_clears_reason_when_not_rejected():
    changes = fb.build_status_update(fb.FeedbackStatusUpdate(status="planned"))
    assert changes["status"] == "planned"
    assert changes["rejection_reason"] is None


def test_build_status_update_rejects_unknown_status():
    with pytest.raises(HTTPException) as exc:
        fb.build_status_update(fb.FeedbackStatusUpdate(status="new"))
    assert exc.value.status_code == 400


def test_validate_feedback_id_requires_positive_integer():
    assert fb.validate_feedback_id("12") == 12
    for bad in ("0", "-3", "abc", None):
        with pytest.raises(HTTPException):
            fb.validate_feedback_id(bad)


# --- Public roadmap projection (privacy) -------------------------------------
def _stored_row(**overrides):
    row = {
        "id": 1,
        "request_type": "feature",
        "message": "Add player splits to profiles",
        "public_title": None,
        "contact_email": "fan@example.com",
        "user_agent": "Mozilla/5.0",
        "admin_notes": "internal note",
        "status": "planned",
        "rejection_reason": None,
        "created_at": "2026-05-01T00:00:00Z",
        "updated_at": "2026-05-02T00:00:00Z",
    }
    row.update(overrides)
    return row


def test_public_roadmap_item_hides_private_fields():
    item = fb.public_roadmap_item(_stored_row())
    assert "contact_email" not in item
    assert "admin_notes" not in item
    assert "user_agent" not in item
    assert item["title"] == "Add player splits to profiles"


def test_public_roadmap_item_includes_reason_only_for_rejected():
    planned = fb.public_roadmap_item(_stored_row(status="planned"))
    assert "rejection_reason" not in planned

    rejected = fb.public_roadmap_item(
        _stored_row(status="rejected", rejection_reason="Duplicate of #4")
    )
    assert rejected["rejection_reason"] == "Duplicate of #4"


def test_group_roadmap_buckets_and_ignores_new():
    rows = [
        _stored_row(id=1, status="planned"),
        _stored_row(id=2, status="shipped"),
        _stored_row(id=3, status="rejected", rejection_reason="No"),
        _stored_row(id=4, status="new"),
    ]
    groups = fb.group_roadmap(rows)
    assert [i["id"] for i in groups["planned"]] == [1]
    assert [i["id"] for i in groups["shipped"]] == [2]
    assert [i["id"] for i in groups["rejected"]] == [3]
    assert "new" not in groups


def test_summarize_message_truncates_long_first_line():
    short = fb.summarize_message("Quick idea\nmore detail")
    assert short == "Quick idea"
    long = fb.summarize_message("y" * 200)
    assert len(long) <= fb.PUBLIC_SUMMARY_LENGTH
    assert long.endswith("…")


# --- Rate limiting -----------------------------------------------------------
def test_rate_limiter_allows_up_to_max_then_blocks_per_key():
    limiter = fb.RateLimiter(max_requests=3, window_seconds=600)
    now = 1000.0
    assert limiter.check("1.2.3.4", now=now) is True
    assert limiter.check("1.2.3.4", now=now) is True
    assert limiter.check("1.2.3.4", now=now) is True
    # Fourth within the window is blocked.
    assert limiter.check("1.2.3.4", now=now) is False
    # A different key has its own budget.
    assert limiter.check("9.9.9.9", now=now) is True


def test_rate_limiter_recovers_after_window_passes():
    limiter = fb.RateLimiter(max_requests=2, window_seconds=600)
    assert limiter.check("1.2.3.4", now=0.0) is True
    assert limiter.check("1.2.3.4", now=1.0) is True
    assert limiter.check("1.2.3.4", now=2.0) is False
    # After the window elapses the earlier hits no longer count.
    assert limiter.check("1.2.3.4", now=700.0) is True


# --- Admin auth --------------------------------------------------------------
class _StubRequest:
    def __init__(self, headers=None, host="1.2.3.4"):
        self.headers = headers or {}
        self.client = type("Client", (), {"host": host})()


def test_request_admin_token_reads_bearer_and_custom_headers():
    assert fb.request_admin_token(_StubRequest({"Authorization": "Bearer abc"})) == "abc"
    assert fb.request_admin_token(_StubRequest({"X-Admin-Token": "xyz"})) == "xyz"
    assert fb.request_admin_token(_StubRequest({})) is None


def test_require_admin_enforces_configured_token(monkeypatch):
    monkeypatch.setattr(fb, "admin_token", lambda: "secret")

    # No token provided -> 403.
    with pytest.raises(HTTPException) as denied:
        fb.require_admin(_StubRequest({}))
    assert denied.value.status_code == 403

    # Wrong token -> 403.
    with pytest.raises(HTTPException):
        fb.require_admin(_StubRequest({"Authorization": "Bearer nope"}))

    # Correct token -> allowed.
    fb.require_admin(_StubRequest({"Authorization": "Bearer secret"}))


def test_require_admin_returns_503_when_token_unset(monkeypatch):
    monkeypatch.setattr(fb, "admin_token", lambda: None)
    with pytest.raises(HTTPException) as exc:
        fb.require_admin(_StubRequest({"Authorization": "Bearer anything"}))
    assert exc.value.status_code == 503


def test_client_ip_prefers_forwarded_header():
    forwarded = _StubRequest({"X-Forwarded-For": "203.0.113.7, 10.0.0.1"})
    assert fb.client_ip(forwarded) == "203.0.113.7"
    assert fb.client_ip(_StubRequest({})) == "1.2.3.4"
