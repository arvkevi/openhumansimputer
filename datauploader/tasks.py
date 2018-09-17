"""
Asynchronous tasks that update data in Open Humans.
These tasks:
  1. delete any current files in OH if they match the planned upload filename
  2. adds a data file
"""
import logging
import os
from django.conf import settings
from open_humans.models import OpenHumansMember
from datetime import datetime
from ohapi import api

# Set up logging.
logger = logging.getLogger(__name__)


def process_source(oh_id):
    oh_member = OpenHumansMember.objects.get(oh_id=oh_id)
    OUT_DIR = os.environ.get('OUT_DIR')
    metadata = {
        'description':
        'Imputed genotypes from Imputer',
        'tags': ['genomics'],
        'updated_at': str(datetime.utcnow()),
    }
    oh_access_token = oh_member.get_access_token(
        client_id=settings.OPENHUMANS_CLIENT_ID,
        client_secret=settings.OPENHUMANS_CLIENT_SECRET)

    # this works below
    try:
        api.delete_file(oh_member.access_token,
                    oh_member.oh_id,
                    file_basename="member.imputed.vcf.bz2")
    except FileNotFoundError :
        """OK, just means new file"""
        pass
    api.upload_aws('{}/{}/member.imputed.vcf.bz2'.format(OUT_DIR, oh_id), metadata,
                   oh_access_token,
                   project_member_id=oh_member.oh_id, max_bytes=256000000)
