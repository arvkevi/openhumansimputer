release: python manage.py migrate
web: gunicorn openhumansimputer.wsgi --log-file -
worker: celery worker -A datauploader --concurrency 1
worker: celery worker -E -A imputer --concurrency 3
