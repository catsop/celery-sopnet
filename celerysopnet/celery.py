from __future__ import absolute_import

from celery import Celery

# Create a Celery application to which task definitions can refer to. The broker
# has to be configured either with a 
app = Celery('celerysopnet', include=['celerysopnet.tasks'])

# Allow the manual start of this application
if __name__ == '__main__':
    app.start()
