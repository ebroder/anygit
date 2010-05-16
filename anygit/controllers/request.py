import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from anygit import models
from anygit.data import exceptions
from anygit.lib.base import BaseController, render
from anygit.lib import helpers

log = logging.getLogger(__name__)

class RequestController(BaseController):
    def index(self):
        return render('/request/index.mako', controller='request')

    def do_request(self):
        url = request.params.get('url')
        if not url:
            helpers.error('You did not provide a URL')
            redirect_to('index')

        if models.Repository.exists(url=url):
            helpers.error('Someone has already requested indexing of %s' % url)
            redirect_to('index')

        models.Repository.create(url=url)
        models.flush()

        if url.startswith('git://'):
            helpers.flash('Successfully requested %s for indexing' % url)
        else:
            helpers.flash('Successfully requested %s for indexing.  However, '
                          'please note that only git protocol (git://) '
                          'repositories are currently supported by anygit.' % url)
        redirect_to('index')
