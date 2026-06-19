"""End-to-end route tests for the feedback API using a fake Supabase client."""

import pytest
from fastapi.testclient import TestClient

import api
from api_routes import feedback as route
from api_services import feedback as fb

ADMIN_TOKEN = "test-admin-token"
ADMIN_HEADERS = {"Authorization": f"Bearer {ADMIN_TOKEN}"}


class _FakeQuery:
    def __init__(self, table):
        self.table = table
        self._insert = None
        self._update = None
        self._eq = {}
        self._in = None

    def insert(self, record):
        self._insert = record
        return self

    def update(self, changes):
        self._update = changes
        return self

    def select(self, *_args, **_kwargs):
        return self

    def in_(self, column, values):
        self._in = (column, values)
        return self

    def eq(self, column, value):
        self._eq[column] = value
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def execute(self):
        store = self.table.rows
        if self._insert is not None:
            row = dict(self._insert)
            row["id"] = self.table.next_id
            self.table.next_id += 1
            store.append(row)
            return _Result([row])
        if self._update is not None:
            updated = []
            for row in store:
                if all(row.get(k) == v for k, v in self._eq.items()):
                    row.update(self._update)
                    updated.append(row)
            return _Result(updated)
        rows = store
        if self._in is not None:
            column, values = self._in
            rows = [r for r in rows if r.get(column) in values]
        for column, value in self._eq.items():
            rows = [r for r in rows if r.get(column) == value]
        return _Result(list(rows))


class _Result:
    def __init__(self, data):
        self.data = data


class _FakeTable:
    def __init__(self):
        self.rows = []
        self.next_id = 1

    def table(self, _name):
        return _FakeQuery(self)


@pytest.fixture
def client(monkeypatch):
    fake = _FakeTable()
    limiter = fb.RateLimiter(fb.FEEDBACK_RATE_LIMIT_MAX, fb.FEEDBACK_RATE_LIMIT_WINDOW_SECONDS)
    route.configure_feedback_dependencies(supabase=fake, rate_limiter=limiter)
    monkeypatch.setattr(fb, "admin_token", lambda: ADMIN_TOKEN)
    test_client = TestClient(api.app)
    test_client.fake = fake
    return test_client


def test_submit_feedback_stores_normalized_row(client):
    response = client.post(
        "/feedback",
        json={"request_type": "Feature Request", "message": "Add splits"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["request_type"] == "feature"
    assert len(client.fake.rows) == 1
    assert client.fake.rows[0]["status"] == "new"


def test_submit_feedback_drops_honeypot_without_storing(client):
    response = client.post(
        "/feedback",
        json={"request_type": "bug", "message": "spam", "website": "http://spam"},
    )
    assert response.status_code == 200
    assert client.fake.rows == []


def test_submit_feedback_validation_errors(client):
    assert client.post("/feedback", json={"request_type": "x", "message": "hi"}).status_code == 400
    assert client.post("/feedback", json={"request_type": "bug", "message": " "}).status_code == 400


def test_submit_feedback_is_rate_limited(client):
    for _ in range(fb.FEEDBACK_RATE_LIMIT_MAX):
        ok = client.post("/feedback", json={"request_type": "bug", "message": "again"})
        assert ok.status_code == 200
    blocked = client.post("/feedback", json={"request_type": "bug", "message": "again"})
    assert blocked.status_code == 429


def test_roadmap_groups_public_items_and_hides_private_fields(client):
    client.fake.rows.extend(
        [
            {"id": 10, "request_type": "feature", "message": "Planned thing",
             "status": "planned", "contact_email": "a@b.com", "admin_notes": "secret",
             "rejection_reason": None, "updated_at": "2026-05-02T00:00:00Z"},
            {"id": 11, "request_type": "bug", "message": "Rejected thing",
             "status": "rejected", "rejection_reason": "Duplicate",
             "contact_email": "c@d.com", "admin_notes": "secret2",
             "updated_at": "2026-05-03T00:00:00Z"},
            {"id": 12, "request_type": "feature", "message": "New untriaged",
             "status": "new", "updated_at": "2026-05-04T00:00:00Z"},
        ]
    )
    client.fake.next_id = 20

    body = client.get("/feedback/roadmap").json()
    assert body["counts"] == {"planned": 1, "shipped": 0, "rejected": 1}
    assert body["rejected"][0]["rejection_reason"] == "Duplicate"
    serialized = str(body)
    assert "a@b.com" not in serialized
    assert "secret" not in serialized


def test_admin_endpoints_require_token(client):
    assert client.get("/feedback/admin").status_code == 403
    assert client.post("/feedback/1/status", json={"status": "planned"}).status_code == 403


def test_admin_can_list_and_update_status(client):
    client.fake.rows.append(
        {"id": 5, "request_type": "feature", "message": "Idea", "status": "new"}
    )
    client.fake.next_id = 6

    listed = client.get("/feedback/admin", headers=ADMIN_HEADERS).json()
    assert listed["count"] == 1

    updated = client.post(
        "/feedback/5/status",
        headers=ADMIN_HEADERS,
        json={"status": "rejected", "rejection_reason": "Out of scope"},
    )
    assert updated.status_code == 200
    assert updated.json()["status"] == "rejected"
    assert client.fake.rows[0]["status"] == "rejected"
    assert client.fake.rows[0]["rejection_reason"] == "Out of scope"


def test_admin_reject_without_reason_is_rejected(client):
    client.fake.rows.append({"id": 7, "message": "x", "status": "new"})
    response = client.post(
        "/feedback/7/status", headers=ADMIN_HEADERS, json={"status": "rejected"}
    )
    assert response.status_code == 400


def test_admin_update_missing_item_returns_404(client):
    response = client.post(
        "/feedback/999/status", headers=ADMIN_HEADERS, json={"status": "planned"}
    )
    assert response.status_code == 404
