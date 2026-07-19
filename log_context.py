import contextvars
import logging

submission_id_var = contextvars.ContextVar("submission_id", default="-")


class SubmissionIdFilter(logging.Filter):
    """Copy the current submission id onto every log record as `submission_id`."""

    def filter(self, record):
        record.submission_id = submission_id_var.get()
        return True
