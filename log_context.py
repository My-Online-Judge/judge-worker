import contextvars
import logging

submission_id_var = contextvars.ContextVar("submission_id", default="-")


class SubmissionIdFilter(logging.Filter):
    """Copy the current submission id onto every log record as `submission_id`, and the worker's name (the pod, under
    Kubernetes) as `worker`."""

    def __init__(self, worker="-"):
        super().__init__()
        self.worker = worker

    def filter(self, record):
        record.submission_id = submission_id_var.get()
        record.worker = self.worker
        return True
