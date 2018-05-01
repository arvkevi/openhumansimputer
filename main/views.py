import logging
<<<<<<< HEAD:datauploader/views.py
=======
import os

from django.conf import settings
from django.contrib.auth import login
from django.shortcuts import redirect, render
>>>>>>> Fix login, add logging setup:oh_data_source/views.py
import requests
import os
import base64
import json
import arrow

from django.contrib.auth import login
from django.shortcuts import render, redirect
from django.conf import settings
from datauploader.tasks import (xfer_to_open_humans, 
                                make_request_respectful_get)
from open_humans.models import OpenHumansMember
from .models import DataSourceMember


# Set up logging.
logger = logging.getLogger(__name__)

<<<<<<< HEAD:datauploader/views.py
=======
APP_BASE_URL = os.getenv('APP_BASE_URL', 'http://127.0.0.1:5000')
>>>>>>> Update views.py:oh_data_source/views.py

<<<<<<< HEAD:datauploader/views.py
def index(request):
    """
    Starting page for app.
    """

    context = {'client_id': settings.OPENHUMANS_CLIENT_ID,
               'oh_proj_page': settings.OH_ACTIVITY_PAGE}
=======
# Set up logging.
logger = logging.getLogger(__name__)

>>>>>>> Fix login, add logging setup:oh_data_source/views.py

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

        # Initiate a data transfer task, then render `complete.html`.
        xfer_to_openhumans.delay(oh_id=oh_member.oh_id)
        
        # Example HTTP get with requests_respectful via celery (in tasks.py)
        # Required arguments are url and realms, but you may pass ANY additional
        # arguments that the "requests" library accepts and they will work as well.
        # make_request_respectful_get.delay('urlhere', realms=["Source"])

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
<<<<<<< HEAD:datauploader/views.py
    req = requests.get(
        '{}/api/direct-sharing/project/exchange-member/'
        .format(settings.OPENHUMANS_OH_BASE_URL),
        params={'access_token': token}
        )
    if req.status_code == 200:
        return req.json()
    raise Exception('Status code {}'.format(req.status_code))
    return None
=======
    logger.debug("Received user returning from Open Humans.")

    # Exchange code for token.
    # This creates an OpenHumansMember and associated User account.
    code = request.GET.get('code', '')
    oh_member = oh_code_to_member(code=code)

    if oh_member:

        # Log in the user.
        # (You may want this if connecting user with another OAuth process.)
        user = oh_member.user
        login(request, user, backend='django.contrib.auth.backends.ModelBackend')

        # Initiate a data transfer task, then render 'complete.html'.
        xfer_to_open_humans.delay(oh_id=oh_member.oh_id)
        context = {'oh_id': oh_member.oh_id,
                   'oh_proj_page': settings.OH_ACTIVITY_PAGE}
        return render(request, 'oh_data_source/complete.html',
                      context=context)

    logger.debug('Invalid code exchange. User returned to starting page.')
    return redirect('/')
>>>>>>> Fix login, add logging setup:oh_data_source/views.py
