"""FastAPI routes for SwordFinder in-app feedback and the public roadmap.

Endpoints:
- ``POST /feedback``                 public submission (validated, honeypot, rate limited)
- ``GET  /feedback/roadmap``         public planned/shipped/rejected groups
- ``GET  /feedback/admin``           operator review list (admin token)
- ``POST /feedback/{id}/status``     operator triage (admin token)

The Supabase client is injected by ``api.py`` through ``configure_feedback_dependencies``
so this module stays import-safe and testable, mirroring the share-x route wiring.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Request

from api_services import feedback as fb

router = APIRouter(prefix="/feedback", tags=["feedback"])

FEEDBACK_TABLE = "feedback"

# Only public-safe columns are read for the roadmap (never contact_email / admin_notes).
PUBLIC_SELECT = (
    "id,request_type,message,public_title,status,rejection_reason,created_at,updated_at"
)
# The operator review endpoint returns the full row, including private fields.
ADMIN_SELECT = (
    "id,request_type,message,contact_email,page_path,page_url,user_agent,theme,"
    "status,rejection_reason,admin_notes,public_title,created_at,updated_at"
)

_supabase = None
_rate_limiter = fb.RateLimiter(
    fb.FEEDBACK_RATE_LIMIT_MAX, fb.FEEDBACK_RATE_LIMIT_WINDOW_SECONDS
)


def configure_feedback_dependencies(*, supabase, rate_limiter: Optional[fb.RateLimiter] = None) -> None:
    global _supabase, _rate_limiter
    _supabase = supabase
    if rate_limiter is not None:
        _rate_limiter = rate_limiter


def _require_supabase():
    if _supabase is None:
        raise RuntimeError("feedback route dependencies are not configured")
    return _supabase


# Message shown to clients before create_feedback_table.sql has been applied,
# instead of leaking the raw Postgres "relation does not exist" error.
STORAGE_UNAVAILABLE_DETAIL = (
    "Feedback storage is not set up yet. Apply create_feedback_table.sql."
)


def _is_missing_table_error(exc: Exception) -> bool:
    """True when the failure is the feedback table not existing yet (PG 42P01)."""
    text = str(exc).lower()
    return "42p01" in text or "does not exist" in text


@router.post("")
async def submit_feedback(request: Request, submission: fb.FeedbackSubmission):
    """Accept a feature request or bug report from any page."""
    supabase = _require_supabase()

    # Honeypot: silently accept and drop without storing so bots get no signal.
    if fb.is_honeypot_triggered(submission):
        return {"ok": True, "status": "received"}

    # Throttle before the (validated) write so a flood of bad payloads is cheap.
    if not _rate_limiter.check(fb.client_ip(request)):
        raise HTTPException(
            status_code=429,
            detail="Too many feedback submissions. Please try again later.",
        )

    record = fb.build_feedback_record(submission)

    try:
        result = supabase.table(FEEDBACK_TABLE).insert(record).execute()
    except Exception as exc:
        if _is_missing_table_error(exc):
            raise HTTPException(status_code=503, detail=STORAGE_UNAVAILABLE_DETAIL)
        raise HTTPException(status_code=500, detail=str(exc))

    inserted = (result.data or [{}])[0]
    return {
        "ok": True,
        "status": "received",
        "id": inserted.get("id"),
        "request_type": record["request_type"],
    }


@router.get("/roadmap")
async def get_roadmap(limit: int = 200):
    """Public roadmap grouped into planned / shipped / rejected."""
    supabase = _require_supabase()
    limit = max(1, min(limit, 500))

    try:
        result = (
            supabase.table(FEEDBACK_TABLE)
            .select(PUBLIC_SELECT)
            .in_("status", list(fb.PUBLIC_STATUSES))
            .order("updated_at", desc=True)
            .limit(limit)
            .execute()
        )
    except Exception as exc:
        # Before the migration is applied the roadmap simply has nothing to show,
        # so render empty groups rather than a 500 on the public page.
        if _is_missing_table_error(exc):
            result = type("Empty", (), {"data": []})()
        else:
            raise HTTPException(status_code=500, detail=str(exc))

    groups = fb.group_roadmap(result.data or [])
    return {
        **groups,
        "counts": {key: len(value) for key, value in groups.items()},
        "last_checked": fb.utc_now_iso(),
    }


@router.get("/admin")
async def list_feedback(request: Request, status: Optional[str] = None, limit: int = 100):
    """Operator review list. Requires the SwordFinder admin token."""
    supabase = _require_supabase()
    fb.require_admin(request)
    limit = max(1, min(limit, 500))

    query = supabase.table(FEEDBACK_TABLE).select(ADMIN_SELECT)
    if status:
        query = query.eq("status", fb.normalize_filter_status(status))

    try:
        result = query.order("created_at", desc=True).limit(limit).execute()
    except Exception as exc:
        if _is_missing_table_error(exc):
            raise HTTPException(status_code=503, detail=STORAGE_UNAVAILABLE_DETAIL)
        raise HTTPException(status_code=500, detail=str(exc))

    rows = result.data or []
    return {
        "count": len(rows),
        "rows": rows,
        "status_counts": fb.count_by_status(rows),
        "last_checked": fb.utc_now_iso(),
    }


@router.post("/{feedback_id}/status")
async def update_feedback_status(
    feedback_id: int, request: Request, update: fb.FeedbackStatusUpdate
):
    """Mark a request planned, shipped, or rejected (with reason). Admin token required."""
    supabase = _require_supabase()
    fb.require_admin(request)

    valid_id = fb.validate_feedback_id(feedback_id)
    changes = fb.build_status_update(update)
    changes["updated_at"] = fb.utc_now_iso()

    try:
        result = (
            supabase.table(FEEDBACK_TABLE)
            .update(changes)
            .eq("id", valid_id)
            .execute()
        )
    except Exception as exc:
        if _is_missing_table_error(exc):
            raise HTTPException(status_code=503, detail=STORAGE_UNAVAILABLE_DETAIL)
        raise HTTPException(status_code=500, detail=str(exc))

    rows = result.data or []
    if not rows:
        raise HTTPException(status_code=404, detail="Feedback item not found")

    return {
        "ok": True,
        "id": valid_id,
        "status": changes["status"],
        "item": rows[0],
    }
