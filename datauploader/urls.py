from django.urls import path

from . import views

urlpatterns = [
    path('datauploader/index/', views.index, name='index'),
    path('datauploader/complete/', views.complete, name='complete'),
]
