import requests
import logging
from django.template.defaulttags import register
from django.contrib import messages
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from django.conf import settings
from open_humans.models import OpenHumansMember
from imputer.tasks import pipeline
from ohapi import api
from imputer.models import ImputerMember


# Set up logging.
logger = logging.getLogger('oh')


def index(request):
    """
    Starting page for app.
    """

    context = {'client_id': settings.OPENHUMANS_CLIENT_ID,
               'redirect_uri': settings.OPENHUMANS_APP_REDIRECT_URI,
               'oh_proj_page': settings.OH_ACTIVITY_PAGE}

    return render(request, 'main/index.html', context=context)


@login_required(login_url="/")
def logout_user(request):
    """
    Logout user
    """
    if request.method == 'POST':
        logout(request)
    return redirect('index')


@login_required(login_url="/")
def delete_user(request):
    if request.method == "POST":
        request.user.delete()
        messages.info(request, "Your account was deleted!")
        logout(request)
    return redirect('index')


@register.filter
def get_item_proj(dictionary, key):
    return dictionary.get(key, {}).get('project')


@register.filter
def get_item_id(dictionary, key):
    return dictionary.get(key, {}).get('id')


@register.filter
def get_item_source_id(dictionary, key):
    return dictionary.get(key, {}).get('source_id')


@login_required(login_url='/')
def dashboard(request):
    oh_member = request.user.oh_member
    context = {
        'oh_member': oh_member,
    }
    try:
        oh_member_data = api.exchange_oauth2_member(
            oh_member.get_access_token())
    except:
        messages.error(request, "You need to re-authenticate with Open Humans")
        logout(request)
        return redirect("/")

    requested_sources = {
        'direct-sharing-128': '23andMe Upload',
        'direct-sharing-129': 'AncestryDNA Upload',
        'direct-sharing-120': 'FamilyTreeDNA integration',
        'direct-sharing-40': 'Gencove',
        'direct-sharing-131': 'Genome/Exome Upload',
        'direct-sharing-139': 'Harvard Personal Genome Project',
        'direct-sharing-55': 'openSNP'
    }

    matching_sources = {}
    found_source_ids = []
    for data_source in oh_member_data['data']:
        if (data_source['source'] in requested_sources and
                'vcf' in data_source['basename']
                and 'metadata' not in data_source['basename']):
            matching_sources[data_source['basename']] = {'project': requested_sources[data_source['source']],
                                                         'id': data_source['id'],
                                                         'source_id': data_source['source']}
            found_source_ids.append(data_source['source'])

    for source_id, source_name in requested_sources.items():
        if source_id not in found_source_ids:
            matching_sources[source_id] = {'project': source_name,
                                           'id': None,
                                           'source_id': source_id}

    # check position in queue
    active_sorted = ImputerMember.objects.filter(active=True).order_by('id')
    queue_position = None
    for index, active in enumerate(active_sorted):
        if oh_member.oh_id == active.oh_id:
            queue_position = index

    context = {
        'base_url': request.build_absolute_uri("/").rstrip('/'),
        'section': 'dashboard',
        'all_datasources': requested_sources,
        'matching_sources': matching_sources,
        'queue_position': queue_position
    }

    return render(request, 'main/dashboard.html',
                  context=context)

def about(request):
    return render(request, 'main/about.html', {'section': 'about'})


def terms(request):
    return render(request, 'main/terms.html', {'section': 'terms'})


def complete(request):
    """
    Receive user from Open Humans. Redirect back to dashboard.
    """
    logger.debug("Received user returning from Open Humans.")
    code = request.GET.get('code', '')
    oh_member = oh_code_to_member(code=code)

    if oh_member:
        # Log in the user.
        user = oh_member.user
        login(request, user,
              backend='django.contrib.auth.backends.ModelBackend')

        return redirect('/dashboard')

    logger.debug('Invalid code exchange. User returned to starting page.')
    return redirect('/')


def launch_imputation(request):
    """
    Logic to check whether user exists:
    If true, alerts member and redirects to Dashboard, otherwise,
    create the user and launch Pipeline
    """
    oh_member = request.user.oh_member
    oh_id = oh_member.oh_id

    vcf_id = request.GET.get('id', '')

    if oh_member:

        exists = len(ImputerMember.objects.filter(oh_id__in=(oh_id,), active=True))
        # should never be higher than 1
        if exists >= 1:
            return redirect('/dashboard?duplicate')
        else:
            new_imputer = ImputerMember(oh_id=oh_id, active=True, step='launch')
            new_imputer.save()

            logger.debug("Launching {}'s pipeline.".format(oh_member.oh_id))

            async_pipeline = pipeline.si(vcf_id, oh_id)
            async_pipeline.apply_async()

            context = {'oh_member': oh_member,
                       'oh_proj_page': settings.OH_ACTIVITY_PAGE}
            return render(request, 'main/complete.html',
                          context=context)

    logger.debug('Oops! User returned to starting page.')
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
            'redirect_uri': settings.OPENHUMANS_APP_REDIRECT_URI,
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
