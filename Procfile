web: gunicorn demotemplate.wsgi --log-file -
worker: celery -A datauploader worker --without-gossip --without-mingle --without-heartbeat
