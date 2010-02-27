from django.conf.urls.defaults import *

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    (r'^$', 'anygit.root.views.index'),
    (r'^q/(?P<query>.*)$', 'anygit.root.views.query'),
)
