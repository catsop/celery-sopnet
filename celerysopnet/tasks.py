from __future__ import absolute_import

from celerysopnet.celery import app

@app.task
def sample_task():
    """
    This task is only used for testing and demonstration.
    """
    return "Sample task finished"
