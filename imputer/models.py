from django.db import models


class ImputerMember(models.Model):
    oh_id = models.CharField(max_length=16)
    step = models.CharField(max_length=10)
    active = models.BooleanField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    data_source_id = models.CharField(max_length=30, default='', null=True, blank=True)

    def __str__(self):
        return 'id: {}\noh_id: {}\nstep: {}\nactive: {}\ncreated_at: {}\nupdated_at: {}'.format(self.id,
            self.oh_id, self.step, self.active, self.created_at, self.updated_at)
