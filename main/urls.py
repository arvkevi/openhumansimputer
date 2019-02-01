from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('about/', views.about, name='about'),
    path('dashboard/', views.dashboard, name='dashboard'),
    path('logout/', views.logout_user, name='logout'),
    path('delete-user/', views.delete_user, name='delete-user'),
    path('launch_imputation/', views.launch_imputation, name='launch-imputation'),
    path('complete/', views.complete, name='complete'),
    path('terms/', views.terms, name='terms'),
]
