from django.contrib.auth.models import User


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
