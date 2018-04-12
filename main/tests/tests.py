from django.test import TestCase, Client
import vcr
from open_humans.models import OpenHumansMember
from django.conf import settings

FILTERSET = [('access_token', 'ACCESSTOKEN')]

my_vcr = vcr.VCR(path_transformer=vcr.VCR.ensure_suffix('.yaml'),
                 cassette_library_dir='main/tests/cassettes',
                 # filter_headers=[('Authorization', 'XXXXXXXX')],
                 filter_query_parameters=FILTERSET,
                 filter_post_data_parameters=FILTERSET)

class LoginTestCase(TestCase):
    """
    Test the login logic of the OH API
    """

    def setUp(self):
        settings.DEBUG = True
        settings.OPENHUMANS_APP_BASE_URL = "http://127.0.0.1"
        # self.invalid_token = 'INVALID_TOKEN'
        # self.master_token = 'ACCESSTOKEN'
        # self.project_info_url = 'https://www.openhumans.org/api/direct-sharing/project/?access_token={}'

    # @my_vcr.use_cassette()
    # def test_complete(self):
    #     c = Client()
    #     self.assertEqual(0,
    #                      OpenHumansMember.objects.all().count())
    #     response = c.get("/complete", {'code': 'yourcodehere'})
    #     self.assertEqual(response.status_code, 200)
    #     self.assertTemplateUsed(response, 'main/complete.html')
    #     self.assertEqual(1,
    #                      OpenHumansMember.objects.all().count())
