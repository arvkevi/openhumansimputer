from django.shortcuts import render
from django.conf import settings


def index(request):
    """
    Starting page for app.
    """
    context = {'client_id': settings.OH_CLIENT_ID,
               'oh_proj_page': settings.OH_ACTIVITY_PAGE}

    return render(request, 'oh_data_source/index.html', context=context)
