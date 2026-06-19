"""Server-side logic for SwordFinder in-app feedback and the public roadmap.

This module holds the pure, easily testable pieces of the feedback feature:
validation + length caps, honeypot detection, per-IP rate limiting, admin-token
auth (sharing the existing SWORDFINDER_ADMIN_TOKEN), and the public/admin row
normalizers. The FastAPI wiring lives in ``api_routes/feedback.py``.
"""

import re
import secrets
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException, Request
from pydantic import BaseModel

from env_config import get_env

# --- Request types -----------------------------------------------------------
REQUEST_TYPES = ("feature", "bug")

# Accept a few human/legacy spellings from clients but always store the canonical
# value so the table CHECK constraint and the roadmap grouping stay simple.
_REQUEST_TYPE_ALIASES = {
    "feature": "feature",
    "feature_request": "feature",
    "feature-request": "feature",
    "feature request": "feature",
    "enhancement": "feature",
    "idea": "feature",
    "bug": "bug",
    "bug_report": "bug",
    "bug-report": "bug",
    "bug report": "bug",
    "issue": "bug",
}

# --- Status / roadmap --------------------------------------------------------
STATUS_NEW = "new"
STATUS_PLANNED = "planned"
STATUS_SHIPPED = "shipped"
STATUS_REJECTED = "rejected"

# Statuses an operator can move a request into (a request starts life as "new").
SETTABLE_STATUSES = (STATUS_PLANNED, STATUS_SHIPPED, STATUS_REJECTED)
# Statuses that appear on the public roadmap.
PUBLIC_STATUSES = (STATUS_PLANNED, STATUS_SHIPPED, STATUS_REJECTED)
# Every status that is valid in the table (used to validate admin list filters).
ALL_STATUSES = (STATUS_NEW, STATUS_PLANNED, STATUS_SHIPPED, STATUS_REJECTED)

# --- Length caps -------------------------------------------------------------
MAX_MESSAGE_LENGTH = 2000
MAX_EMAIL_LENGTH = 254
MAX_CONTEXT_LENGTH = 500  # page_path / page_url / user_agent
MAX_THEME_LENGTH = 40
MAX_REASON_LENGTH = 1000
MAX_NOTES_LENGTH = 2000
MAX_TITLE_LENGTH = 140
PUBLIC_SUMMARY_LENGTH = 140

# --- Rate limiting -----------------------------------------------------------
FEEDBACK_RATE_LIMIT_MAX = 5
FEEDBACK_RATE_LIMIT_WINDOW_SECONDS = 600  # 10 minutes per IP

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

ADMIN_REQUIRED_DETAIL = "This endpoint requires a SwordFinder admin token."


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# --- Pydantic payloads -------------------------------------------------------
class FeedbackSubmission(BaseModel):
    request_type: Optional[str] = None
    message: Optional[str] = None
    contact_email: Optional[str] = None
    # Captured automatically by the launcher.
    page_path: Optional[str] = None
    page_url: Optional[str] = None
    user_agent: Optional[str] = None
    theme: Optional[str] = None
    # Honeypot: a real user never sees or fills this. Bots that auto-fill forms do.
    website: Optional[str] = None


class FeedbackStatusUpdate(BaseModel):
    status: Optional[str] = None
    rejection_reason: Optional[str] = None
    admin_notes: Optional[str] = None
    public_title: Optional[str] = None


# --- Validation --------------------------------------------------------------
def normalize_request_type(value: Optional[str]) -> str:
    key = str(value or "").strip().lower()
    resolved = _REQUEST_TYPE_ALIASES.get(key)
    if resolved not in REQUEST_TYPES:
        raise HTTPException(status_code=400, detail="request_type must be 'feature' or 'bug'")
    return resolved


def validate_message(value: Optional[str]) -> str:
    message = str(value or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message is required")
    if len(message) > MAX_MESSAGE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"message must be {MAX_MESSAGE_LENGTH} characters or fewer",
        )
    return message


def validate_contact_email(value: Optional[str]) -> Optional[str]:
    email = str(value or "").strip()
    if not email:
        return None
    if len(email) > MAX_EMAIL_LENGTH or not EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="contact_email must be a valid email address")
    return email.lower()


def clean_context(value: Optional[str], limit: int = MAX_CONTEXT_LENGTH) -> Optional[str]:
    """Trim and length-cap free-form page context; never raise on it."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:limit]


def is_honeypot_triggered(submission: FeedbackSubmission) -> bool:
    return bool((submission.website or "").strip())


def build_feedback_record(submission: FeedbackSubmission) -> dict:
    """Validate a submission and return the row to insert (raises on bad input)."""
    return {
        "request_type": normalize_request_type(submission.request_type),
        "message": validate_message(submission.message),
        "contact_email": validate_contact_email(submission.contact_email),
        "page_path": clean_context(submission.page_path),
        "page_url": clean_context(submission.page_url),
        "user_agent": clean_context(submission.user_agent),
        "theme": clean_context(submission.theme, MAX_THEME_LENGTH),
        "status": STATUS_NEW,
    }


def validate_feedback_id(feedback_id) -> int:
    try:
        parsed = int(feedback_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="feedback id must be a positive integer")
    if parsed <= 0:
        raise HTTPException(status_code=400, detail="feedback id must be a positive integer")
    return parsed


def normalize_settable_status(value: Optional[str]) -> str:
    status = str(value or "").strip().lower()
    if status not in SETTABLE_STATUSES:
        raise HTTPException(status_code=400, detail="status must be planned, shipped, or rejected")
    return status


def normalize_filter_status(value: Optional[str]) -> str:
    status = str(value or "").strip().lower()
    if status not in ALL_STATUSES:
        raise HTTPException(
            status_code=400,
            detail="status must be new, planned, shipped, or rejected",
        )
    return status


def build_status_update(update: FeedbackStatusUpdate) -> dict:
    """Validate an operator status change and return the column changes to apply."""
    status = normalize_settable_status(update.status)
    changes: dict = {"status": status}

    reason = (update.rejection_reason or "").strip()
    if status == STATUS_REJECTED:
        if not reason:
            raise HTTPException(
                status_code=400,
                detail="rejection_reason is required when rejecting feedback",
            )
        if len(reason) > MAX_REASON_LENGTH:
            raise HTTPException(
                status_code=400,
                detail=f"rejection_reason must be {MAX_REASON_LENGTH} characters or fewer",
            )
        changes["rejection_reason"] = reason
    else:
        # Clear any stale reason when a request leaves the rejected state.
        changes["rejection_reason"] = None

    if update.admin_notes is not None:
        notes = update.admin_notes.strip()
        changes["admin_notes"] = notes[:MAX_NOTES_LENGTH] or None

    if update.public_title is not None:
        title = update.public_title.strip()
        changes["public_title"] = title[:MAX_TITLE_LENGTH] or None

    return changes


# --- Public roadmap normalization -------------------------------------------
def summarize_message(message: Optional[str], limit: int = PUBLIC_SUMMARY_LENGTH) -> str:
    text = str(message or "").strip()
    first_line = text.splitlines()[0].strip() if text else ""
    if len(first_line) <= limit:
        return first_line
    return first_line[: limit - 1].rstrip() + "…"


def public_roadmap_item(row: dict) -> dict:
    """Project a stored row down to public fields only.

    Never exposes contact_email, admin_notes, user_agent, or page context.
    rejection_reason is included only for rejected items.
    """
    status = row.get("status")
    message = row.get("message")
    item = {
        "id": row.get("id"),
        "request_type": row.get("request_type"),
        "status": status,
        "title": (row.get("public_title") or summarize_message(message)),
        "message": message,
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }
    if status == STATUS_REJECTED:
        item["rejection_reason"] = row.get("rejection_reason")
    return item


def group_roadmap(rows: list) -> dict:
    groups = {STATUS_PLANNED: [], STATUS_SHIPPED: [], STATUS_REJECTED: []}
    for row in rows or []:
        status = row.get("status")
        if status in groups:
            groups[status].append(public_roadmap_item(row))
    return groups


def count_by_status(rows: list) -> dict:
    counts = {status: 0 for status in ALL_STATUSES}
    for row in rows or []:
        status = row.get("status")
        if status in counts:
            counts[status] += 1
    return counts


# --- Admin auth (shares SWORDFINDER_ADMIN_TOKEN with the X tooling) ----------
def admin_token() -> Optional[str]:
    return get_env("SWORDFINDER_ADMIN_TOKEN") or get_env("X_POST_ADMIN_TOKEN")


def _header_value(request: Request, name: str) -> Optional[str]:
    headers = getattr(request, "headers", {}) or {}
    if hasattr(headers, "get"):
        value = headers.get(name) or headers.get(name.lower())
        if value is not None:
            return value
    items = getattr(headers, "items", lambda: [])()
    for key, value in items:
        if str(key).lower() == name.lower():
            return value
    return None


def request_admin_token(request: Request) -> Optional[str]:
    authorization = (_header_value(request, "Authorization") or "").strip()
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip() or None
    for header_name in ("X-SwordFinder-Admin-Token", "X-Admin-Token"):
        token = (_header_value(request, header_name) or "").strip()
        if token:
            return token
    return None


def request_is_admin(request: Request) -> bool:
    configured = admin_token()
    provided = request_admin_token(request)
    return bool(
        configured
        and provided
        and secrets.compare_digest(provided, configured)
    )


def require_admin(request: Request) -> None:
    if not admin_token():
        raise HTTPException(
            status_code=503,
            detail=f"{ADMIN_REQUIRED_DETAIL} Set SWORDFINDER_ADMIN_TOKEN.",
        )
    if not request_is_admin(request):
        raise HTTPException(status_code=403, detail=ADMIN_REQUIRED_DETAIL)


# --- Rate limiting -----------------------------------------------------------
class RateLimiter:
    """A tiny in-memory sliding-window limiter keyed by client IP.

    State is process-local, which matches the existing in-memory OAuth session
    store in ``x_sharing``. It throttles casual abuse; durable cross-instance
    limiting would need a shared store.
    """

    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict = {}

    def check(self, key: str, now: Optional[float] = None) -> bool:
        """Return True and record the hit when allowed; False when over the limit."""
        if now is None:
            now = time.monotonic()
        window_start = now - self.window_seconds
        hits = [t for t in self._hits.get(key, []) if t > window_start]
        if len(hits) >= self.max_requests:
            self._hits[key] = hits
            return False
        hits.append(now)
        self._hits[key] = hits
        return True

    def reset(self) -> None:
        self._hits.clear()


def client_ip(request: Request) -> str:
    """Best-effort client IP, honoring the first X-Forwarded-For hop (Railway)."""
    forwarded = (_header_value(request, "X-Forwarded-For") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    client = getattr(request, "client", None)
    host = getattr(client, "host", None) if client else None
    return host or "unknown"
