import ohapi.api as api

from django.test import TestCase
from imputer.tasks import pipeline
from imputer.models import ImputerMember


class PipelineTestCase(TestCase):
    def setUp(self):
        pass

    def test_pipeline(self):
        for vcf_id, oh_id in zip([29230, 474017, 466912], ['77687130', '32187646', '02080045']):
            new_imputer = ImputerMember(
                oh_id=oh_id, active=True, step='launch')
            new_imputer.save()

            async_pipeline = pipeline.si(vcf_id, oh_id)
            async_pipeline.apply_async()
