from django.shortcuts import render
import demotemplate.settings


def index(request):
    """
    Starting page for app.
    """
    context = {'client_id': demotemplate.settings.OH_CLIENT_ID,
               'oh_proj_page': demotemplate.settings.OH_ACTIVITY_PAGE}

    return render(request, 'oh_data_source/index.html', context=context)
