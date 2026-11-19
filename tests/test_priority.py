"""user_priority: the negotiator's fair-share records joined with the submitter ads."""

from __future__ import annotations

import getpass

import pytest
from fastmcp.exceptions import ToolError

from conftest import call, fake_priorities

pytestmark = pytest.mark.anyio
ME = getpass.getuser()


async def test_user_priority_me(live_jobs):
    out = await call("user_priority")
    assert out["as_of"]
    assert out["users"], out
    for row in out["users"]:
        assert row["user"].split("@")[0].split(".")[-1] == ME
    plain = next(row for row in out["users"] if row["user"].startswith(f"{ME}@"))
    assert plain["effective_priority"] == pytest.approx(plain["real_priority"] * plain["priority_factor"], rel=1e-2)
    # Submitter ads trail the queue by up to SCHEDD_INTERVAL, so only their shape is checked
    assert set(plain["jobs"]) == {"idle", "running", "held"}
    assert plain["rank"] >= 1


async def test_user_priority_all(live_jobs):
    out = await call("user_priority", owner="all")
    assert any(row["user"].startswith(f"{ME}@") for row in out["users"])
    for row in out["users"]:
        assert sum(row["jobs"].values()) > 0, row  # only users with jobs in a queue
    priorities = [row["effective_priority"] for row in out["users"] if "effective_priority" in row]
    assert priorities == sorted(priorities)


async def test_user_priority_unknown_user():
    out = await call("user_priority", owner="condor-mcp-nobody")
    assert out["users"] == []
    assert "no record of condor-mcp-nobody" in out["note"]


def test_negotiator_answer_is_reused(monkeypatch):
    from condor_mcp import pool

    ads, read_at, warning = pool.accountant_ads()
    assert warning is None and pool.accountant_ads().read_at == read_at
    monkeypatch.setattr(pool, "_priorities", (read_at - pool.PRIORITY_MAX_AGE - 1, ads))
    assert pool.accountant_ads().read_at > read_at - pool.PRIORITY_MAX_AGE - 1


def _failing_negotiator(monkeypatch):
    """Make every negotiator read fail, and count the attempts."""
    from condor_mcp import pool

    attempts = []

    def fail():
        attempts.append(1)
        raise ToolError("The collector has no negotiator ad.")

    monkeypatch.setattr(pool, "locate_negotiator", fail)
    return attempts


async def test_stale_answer_served_when_the_negotiator_fails(monkeypatch):
    """An old answer is better than none: it comes back with its as_of and a warning,
    and the failure is not retried on every call."""
    from condor_mcp import pool

    ads, read_at, _ = pool.accountant_ads()
    monkeypatch.setattr(pool, "_priorities", (read_at - pool.PRIORITY_MAX_AGE - 1, ads))
    attempts = _failing_negotiator(monkeypatch)
    out = await call("user_priority")
    assert out["users"] and "no negotiator ad" in out["warnings"][0] and "last answer" in out["warnings"][0]
    assert (await call("user_priority"))["warnings"] == out["warnings"]
    assert len(attempts) == 1


def test_failure_is_remembered_then_retried(monkeypatch):
    from condor_mcp import pool

    attempts = _failing_negotiator(monkeypatch)
    for _ in range(2):
        with pytest.raises(ToolError, match="no negotiator ad"):
            pool.accountant_ads()
    assert len(attempts) == 1
    when, message = pool._priority_error
    monkeypatch.setattr(pool, "_priority_error", (when - pool.PRIORITY_RETRY_AFTER - 1, message))
    with pytest.raises(ToolError, match="no negotiator ad"):
        pool.accountant_ads()
    assert len(attempts) == 2


def test_missing_negotiator_does_not_fall_back_to_local_config(monkeypatch):
    """A configured collector with no negotiator ad must not mean the local config's
    negotiator, which may belong to another pool."""
    from condor_mcp import pool, worker

    monkeypatch.setattr(pool, "settings", pool.settings.model_copy(update={"collector": "pool-a.example.edu"}))
    monkeypatch.setattr(pool, "call", lambda fn, *args: None if fn is worker.collector_locate else pytest.fail())
    with pytest.raises(ToolError, match="no negotiator ad"):
        pool.locate_negotiator()


async def test_rank_counts_only_users_waiting_ahead(monkeypatch):
    """Rank counts users with idle jobs and a better priority, a user with no record
    counts as the best, group records are left out, and a user's accounting-group
    records are theirs only when their queued jobs run under that group."""
    fake_priorities(
        monkeypatch,
        records=[
            ("alice@d", 900.0),
            ("group_x.alice@d", 300.0),
            ("group_y.alice@d", 100.0),  # no queued jobs under it: not shown
            ("bob@d", 200.0),
            ("carol@d", 100.0),
            ("dave@d", 50.0),
            ("frank@d", 10.0),
            ("<none>", 1.0, True),
        ],
        submitters=[
            ("alice@d", 5),
            ("group_x.alice@d", 0, 1),
            ("bob@d", 0, 2),
            ("bob@d", 1),  # a second schedd
            ("carol@d", 0, 0, 3),  # held only, so not waiting
            ("frank@d", 0, 4),  # running only, so not waiting
            ("erin@d", 1),  # no record yet
        ],
        group_jobs=[("alice@d", "group_x.alice")],
    )

    out = await call("user_priority", owner="alice")
    assert out["users_competing"] == 3
    assert [(row["user"], row["rank"]) for row in out["users"]] == [("group_x.alice@d", 3), ("alice@d", 3)]
    assert out["users"][1]["usage_hours"] == 2.0

    out = await call("user_priority", owner="all")
    users = {row["user"]: row for row in out["users"]}
    assert list(users) == ["erin@d", "frank@d", "carol@d", "bob@d", "group_x.alice@d", "alice@d"]
    assert users["bob@d"]["jobs"] == {"idle": 1, "running": 2, "held": 0}
    assert users["bob@d"]["rank"] == 2  # behind erin, who has no record
    assert users["erin@d"]["rank"] == 1 and "no usage record" in users["erin@d"]["note"]


async def test_dotted_username_is_another_user(monkeypatch):
    """john.smith@d is not smith's accounting-group record, however it looks."""
    fake_priorities(
        monkeypatch,
        records=[("smith@d", 500.0), ("john.smith@d", 100.0), ("group_a.smith@d", 200.0)],
        submitters=[("smith@d", 1), ("john.smith@d", 4), ("group_a.smith@d", 1)],
        group_jobs=[("smith@d", "group_a.smith")],
    )
    out = await call("user_priority", owner="smith")
    assert sorted(row["user"] for row in out["users"]) == ["group_a.smith@d", "smith@d"]


async def test_unreadable_queue_keeps_the_plain_record(monkeypatch):
    from condor_mcp import pool

    fake_priorities(monkeypatch, records=[("smith@d", 500.0), ("group_a.smith@d", 200.0)], submitters=[])

    def fail(constraint, projection):
        raise ToolError("schedd (local): no answer")

    monkeypatch.setattr(pool, "query_jobs", fail)
    out = await call("user_priority", owner="smith")
    assert [row["user"] for row in out["users"]] == ["smith@d"]
    assert "accounting groups" in out["warnings"][0]
