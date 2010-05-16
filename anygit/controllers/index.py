import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from anygit.lib import helpers
from anygit.lib.base import BaseController, render
from anygit import models

log = logging.getLogger(__name__)

class IndexController(BaseController):
    def index(self):
        return render('index.mako', controller='index')

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
