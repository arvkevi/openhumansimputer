from django.contrib import admin

from .models import ImputerMember

# Register your models here.


@admin.register(ImputerMember)
class ImputerMemberAdmin(admin.ModelAdmin):
    fields = ('oh_id', 'step', 'active', 'data_source_id')

    list_display = ('oh_id', 'step', 'active', 'data_source_id', 'created_at', 'updated_at')