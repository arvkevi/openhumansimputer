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

CELERY_BROKER_URL = os.getenv('REDIS_URL')

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE',
                      'demotemplate.settings')

app = Celery('datauploader', broker=CELERY_BROKER_URL)
# Set up Celery with Heroku CloudAMQP (or AMQP in local dev).
app.conf.update({
    'BROKER_URL': CELERY_BROKER_URL,
    # Recommended settings. See: https://www.cloudamqp.com/docs/celery.html
    'BROKER_POOL_LIMIT': None,
    'BROKER_HEARTBEAT': None,
    'BROKER_CONNECTION_TIMEOUT': 30,
    'CELERY_RESULT_BACKEND': CELERY_BROKER_URL,
    'CELERY_SEND_EVENTS': False,
    'CELERY_EVENT_QUEUE_EXPIRES': 60,
})


# Using a string here means the worker will not have to
# pickle the object when using Windows.
app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
