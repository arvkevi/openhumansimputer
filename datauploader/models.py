from django.contrib.auth.models import User
from django.db import models


class OpenHumansMember(models.Model):
    """
    Store OAuth2 data for Open Humans member.
    A user account is created for this Open Humans member.
    """


def make_unique_username(base):
    try:
        User.objects.get(username=base)
    except User.DoesNotExist:
        return base
    n = 2
    while True:
        name = base + str(n)
        try:
            User.objects.get(username=name)
            n += 1
        except User.DoesNotExist:
            return name
