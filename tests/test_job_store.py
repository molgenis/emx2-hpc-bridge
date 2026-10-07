import pytest

from emx2_hpc_bridge.job_store import JobStatus, JobStore


@pytest.fixture
def store():
    store = JobStore(":memory:")
    yield store
    store.close()


def test_add_claimed_sets_status_and_timestamp(store):
    store.add_claimed("job-1")
    job = store.get("job-1")
    assert job.job_id == "job-1"
    assert job.status is JobStatus.CLAIMED
    assert job.claimed_at.tzinfo is not None


def test_get_returns_none_for_unknown_job(store):
    assert store.get("missing") is None


def test_add_claimed_twice_keeps_existing_record(store):
    first = store.add_claimed("job-1")
    store.update_status("job-1", JobStatus.SUBMITTED)
    second = store.add_claimed("job-1")
    assert second.claimed_at == first.claimed_at
    assert second.status is JobStatus.SUBMITTED


def test_update_status(store):
    store.add_claimed("job-1")
    store.update_status("job-1", JobStatus.SUBMITTED)
    assert store.get("job-1").status is JobStatus.SUBMITTED


def test_update_status_unknown_job_raises(store):
    with pytest.raises(KeyError):
        store.update_status("missing", JobStatus.COMPLETED)


def test_list_by_status(store):
    store.add_claimed("job-1")
    store.add_claimed("job-2")
    store.update_status("job-2", JobStatus.COMPLETED)
    assert [j.job_id for j in store.list_by_status(JobStatus.CLAIMED)] == ["job-1"]
    assert [j.job_id for j in store.list_by_status(JobStatus.COMPLETED)] == ["job-2"]
