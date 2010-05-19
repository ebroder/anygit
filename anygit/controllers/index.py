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
        url = request.params.get('url').strip()
        if not url:
            helpers.error('You did not provide a URL')
            redirect_to('index')

        if models.Repository.exists(url=url):
            repo = models.Repository.get_by_attributes(url=url)
            if repo.approved:
                helpers.flash('Someone has already requested indexing of %s, '
                              'so no worries' % url)
            else:
                if not url.startswith('git://'):
                    helpers.flash("That repo (%s) has been already requested.  At the "
                                  "moment, anygit only supports git protocol (git://) "
                                  "repositories.  Once we've added support for this "
                                  "repo's protocol, we'll index it." % url)
                elif not fetch.check_validity(repo):
                    helpers.error("That's odd... someone already asked for %s, but it looks "
                                  "to us like we can't talk to that repo.  Is there a typo "
                                  "in there?  If not, please email anygit@mit.edu" % url)
                else:
                    repo.approved = True
                    repo.save()
                    models.flush()
                    helpers.flash("Someone had requested %s before but it was down then. "
                                  "Looks like it's back up now.  We'll get right to it."
                                  % url)
            redirect_to('index')

        repo = models.Repository.create(url=url)

        if not url.startswith('git://'):
            helpers.flash('Successfully requested %s for future indexing.  However, '
                          'please note that only git protocol (git://) '
                          'repositories are currently supported by anygit.' % url)
        # Make sure we can talk to it
        elif not fetch.check_validity(repo):
            helpers.error("Could not talk to %s; are you sure it's a valid URL?" % url)
        else:
            repo.approved = True
            repo.save()
            helpers.flash('Successfully requested %s for indexing' % url)
        redirect_to('index')
