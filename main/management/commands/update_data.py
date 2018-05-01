from django.core.management.base import BaseCommand
from main.models import DataSourceMember


class Command(BaseCommand):
    help = 'Updates data for all members'

    def handle(self, *args, **options):
        users = DataSourceMember.objects.all()
        for user in users:
            # Uncomment "pass" and add an "update data" or "fetch data" function
            # that will update a users data.
            pass