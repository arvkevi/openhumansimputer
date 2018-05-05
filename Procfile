release: python manage.py migrate
web: gunicorn demotemplate.wsgi --log-file -
worker: celery worker -A datauploader --concurrency 1
