"""
Celery set up, as recommended by celery
http://celery.readthedocs.org/en/latest/django/first-steps-with-django.html

Celery will automatically discover and use methods within INSTALLED_APPs that
have the @shared_task decorator.
"""
# absolute_import prevents conflicts between project celery.py file
# and the celery package.
from __future__ import absolute_import

import os

from celery import Celery

from django.conf import settings

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE',
                      'openhumansimputer.settings')

app = Celery('openhumansimputer', broker=settings.CELERY_BROKER_URL, backend=settings.CELERY_BROKER_URL)
#app.conf.CELERY_ALWAYS_EAGER = True
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

app.conf.update({
    # Recommended settings. See: https://www.cloudamqp.com/docs/celery.html
    'BROKER_POOL_LIMIT': 1,
    'BROKER_HEARTBEAT': None,
    'BROKER_CONNECTION_TIMEOUT': 30,
    'CELERY_RESULT_BACKEND': settings.CELERY_BROKER_URL,
    'CELERY_SEND_EVENTS': True,
    'CELERY_EVENT_QUEUE_EXPIRES': 60,
    'CELERYD_PREFETCH_MULTIPLIER': 0,
    'CELERY_IGNORE_RESULT': True,
})


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
