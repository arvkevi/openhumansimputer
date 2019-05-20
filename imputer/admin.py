import logging
import time

from django.contrib import admin
from .models import ImputerMember
from .tasks import pipeline

logger = logging.getLogger('oh')

# Register your models here.
@admin.register(ImputerMember)
class ImputerMemberAdmin(admin.ModelAdmin):
    fields = ('oh_id', 'step', 'active', 'data_source_id')

    list_display = ('oh_id', 'step', 'active', 'data_source_id', 'created_at', 'updated_at')

    actions = ['reset_pipeline']

    def reset_pipeline(self, request, queryset):
        for member in queryset:
            logger.critical(f'Launching pipeline for {member.oh_id}')
            async_pipeline = pipeline.si(member.data_source_id, member.oh_id)
            async_pipeline.apply_async()
            time.sleep(5)