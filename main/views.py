import requests
from celery import signature, chord
import logging
import os

from django.contrib.auth import login
from django.shortcuts import render, redirect
from django.conf import settings
from imputerlauncher.tasks import (submit_chrom, get_vcf, prepare_data, combine_chrom)
from datauploader.tasks import (xfer_to_open_humans, make_request_respectful_get)
from open_humans.models import OpenHumansMember


# in production this should be False
TEST_CHROMS = os.environ.get('TEST_CHROMS')

# Set up logging.
logger = logging.getLogger(__name__)


def index(request):
    """
    Starting page for app.
    """

    context = {'client_id': settings.OPENHUMANS_CLIENT_ID,
               'redirect_uri': settings.OPENHUMANS_APP_REDIRECT_URI,
               'oh_proj_page': settings.OH_ACTIVITY_PAGE}

    return render(request, 'main/index.html', context=context)


def complete(request):
    """
    Receive user from Open Humans. Store data, start upload.
    """
    logger.debug("Received user returning from Open Humans.")
    # Exchange code for token.
    # This creates an OpenHumansMember and associated user account.
    code = request.GET.get('code', '')
    oh_member = oh_code_to_member(code=code)

    if oh_member:
        # Log in the user.
        user = oh_member.user
        login(request, user,
              backend='django.contrib.auth.backends.ModelBackend')

        # get the member's vcf file
        logger.debug('downloading {}\'s .vcf file.'.format(oh_member.oh_id))
        get_vcf(oh_member)
        # convert to plink format
        prepare_data()

        if TEST_CHROMS:
            CHROMOSOMES = ["{}".format(i) for i in list(range(20, 23))] #+ ["23"]
        else:
            CHROMOSOMES = ["{}".format(i) for i in list(range(1, 23))] #+ ["23"]

        signature('shared_tasks.apply_async', countdown=10)
        res = chord((submit_chrom.s(chrom) for chrom in CHROMOSOMES), combine_chrom.s())()

        context = {'oh_id': oh_member.oh_id,
                   'oh_proj_page': settings.OH_ACTIVITY_PAGE}
        return render(request, 'main/complete.html',
                      context=context)

    logger.debug('Invalid code exchange. User returned to starting page.')
    return redirect('/')


def oh_code_to_member(code):
    """
    Exchange code for token, use this to create and return OpenHumansMember.
    If a matching OpenHumansMember exists, update and return it.
    """
    if settings.OPENHUMANS_CLIENT_SECRET and \
       settings.OPENHUMANS_CLIENT_ID and code:
        data = {
            'grant_type': 'authorization_code',
            'redirect_uri':
            '{}/complete'.format(settings.OPENHUMANS_APP_BASE_URL),
            'code': code,
        }
        req = requests.post(
            '{}/oauth2/token/'.format(settings.OPENHUMANS_OH_BASE_URL),
            data=data,
            auth=requests.auth.HTTPBasicAuth(
                settings.OPENHUMANS_CLIENT_ID,
                settings.OPENHUMANS_CLIENT_SECRET
            )
        )
        data = req.json()

        if 'access_token' in data:
            oh_id = oh_get_member_data(
                data['access_token'])['project_member_id']
            try:
                oh_member = OpenHumansMember.objects.get(oh_id=oh_id)
                logger.debug('Member {} re-authorized.'.format(oh_id))
                oh_member.access_token = data['access_token']
                oh_member.refresh_token = data['refresh_token']
                oh_member.token_expires = OpenHumansMember.get_expiration(
                    data['expires_in'])
            except OpenHumansMember.DoesNotExist:
                oh_member = OpenHumansMember.create(
                    oh_id=oh_id,
                    access_token=data['access_token'],
                    refresh_token=data['refresh_token'],
                    expires_in=data['expires_in'])
                logger.debug('Member {} created.'.format(oh_id))
            oh_member.save()

            return oh_member

        elif 'error' in req.json():
            logger.debug('Error in token exchange: {}'.format(req.json()))
        else:
            logger.warning('Neither token nor error info in OH response!')
    else:
        logger.error('OH_CLIENT_SECRET or code are unavailable')
    return None


def oh_get_member_data(token):
    """
    Exchange OAuth2 token for member data.
    """
    req = requests.get(
        '{}/api/direct-sharing/project/exchange-member/'
        .format(settings.OPENHUMANS_OH_BASE_URL),
        params={'access_token': token}
        )
    if req.status_code == 200:
        return req.json()
    raise Exception('Status code {}'.format(req.status_code))
    return None
