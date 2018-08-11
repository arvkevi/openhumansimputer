"""
Asynchronous tasks that update data in Open Humans.
These tasks:
  1. delete any current files in OH if they match the planned upload filename
  2. adds a data file
"""
import logging
import json
import tempfile
import requests
import os
from celery import shared_task
from django.conf import settings
from open_humans.models import OpenHumansMember
from datetime import datetime, timedelta
from openhumansimputer.settings import rr
from requests_respectful import RequestsRespectfulRateLimitedError
from ohapi import api
import arrow

# Set up logging.
logger = logging.getLogger(__name__)


@shared_task
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

    #source_data = get_existing_data(oh_access_token)
    source_data = None
    #datasource_member = oh_member.datasourcemember

    update_datasource(oh_member, source_data)

    # this works below
    # api.upload_aws('{}/member.imputed.vcf'.format(OUT_DIR), metadata,
    #               oh_access_token,
    #               project_member_id=oh_member.oh_id)


@shared_task
def make_request_respectful_get(url, realms, **kwargs):
    r = rr.get(url=url, realms=realms, **kwargs)
    logger.debug('Request completed. Response: {}'.format(r.text))


def update_datasource(oh_member, source_data):
    try:
        # 1. Set start and end times for API calls- may have to loop over short periods.
        # 2. Get data from API using requests_respectful:

        # r = rr.get(url=url, realms=realms, **kwargs)
        # logger.debug('Request completed. Response: {}'.format(r.text))
        # source_data += r.json()

        print('successfully finished update for {}'.format(oh_member.oh_id))
        #datasource_member = oh_member.datasourcemember
        #datasource_member.last_updated = arrow.now().format()
        # datasource_member.save()
    except RequestsRespectfulRateLimitedError:
        logger.debug(
            'requeued processing for {} with 60 secs delay'.format(
                oh_member.oh_id)
        )
        process_source.apply_async(args=[oh_member.oh_id], countdown=61)
    finally:
        replace_datasource(oh_member, source_data)


def replace_datasource(oh_member, source_data):
    OUT_DIR = os.environ.get('OUT_DIR')
    metadata = {
        'description':
        'Imputed genotypes from Imputer',
        'tags': ['genomics'],
        'updated_at': str(datetime.utcnow()),
    }
    oh_access_token = oh_member.get_access_token()
    logger.debug('deleted old file for {}'.format(oh_member.oh_id))
    api.delete_file(oh_member.access_token,
                    oh_member.oh_id,
                    file_basename="member.imputed.vcf")
    api.upload_aws('{}/member.imputed.vcf'.format(OUT_DIR), metadata,
                   oh_access_token,
                   project_member_id=oh_member.oh_id)
    logger.debug('uploaded new file for {}'.format(oh_member.oh_id))


def get_start_date(source_data):
    # This function should get a start date for data
    # retrieval, by using the data source API.
    pass


def get_existing_data(oh_access_token):
    member = api.exchange_oauth2_member(oh_access_token)
    for dfile in member['data']:
        if 'genomics' in dfile['metadata']['tags']:
            # get file here and read the json into memory
            tf_in = tempfile.NamedTemporaryFile(suffix='.vcf')
            tf_in.write(requests.get(dfile['download_url']).content)
            tf_in.flush()
            with open(tf_in.name) as f:
                existing_file = f.readlines()
            return existing_file
    return []
