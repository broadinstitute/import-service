from functions import chunk_task


def test_chunk_task():
    fetch = chunk_task.handle({"job_id": "four"})
    assert fetch == []
