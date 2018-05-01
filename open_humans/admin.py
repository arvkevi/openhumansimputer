from django.contrib import admin

from .models import OpenHumansMember

# Register your models here.


@admin.register(OpenHumansMember)
class OpenHumansMemberAdmin(admin.ModelAdmin):
    pass
