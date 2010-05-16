import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from anygit.lib import helpers
from anygit.lib.base import BaseController, render
from anygit import models
from anygit.client import fetch

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
            r = models.Repository.get_by_attributes(url=url)
            if r.approved:
                helpers.flash('Someone has already requested indexing of %s, '
                              'so no worries' % url)
            else:
                helpers.error("That's odd... someone already asked for %s, but it looks "
                              "to use like we can't talk to that repo.  Is there a typo "
                              "in there?  If not, please email anygit@mit.edu" % url)
            redirect_to('index')

        repo = models.Repository.create(url=url)
        # Make sure we can talk to it
        if not fetch.check_validity(repo):
            repo.approved = False
            repo.save()
            models.flush()
            helpers.error("Could not talk to %s; are you sure it's a valid URL?" % url)
            redirect_to('index')

        models.flush()
        if url.startswith('git://'):
            helpers.flash('Successfully requested %s for indexing' % url)
        else:
            helpers.flash('Successfully requested %s for indexing.  However, '
                          'please note that only git protocol (git://) '
                          'repositories are currently supported by anygit.' % url)
        redirect_to('index')
