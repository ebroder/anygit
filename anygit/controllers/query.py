import logging
import re

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from anygit.lib import helpers
from anygit.lib.base import BaseController, render
from anygit import models

log = logging.getLogger(__name__)
sha1_re = re.compile('^[a-fA-F0-9]*$')

class QueryController(BaseController):
    def index(self):
        return render('index.mako', controller='query')

    def query(self, id, limit=''):
        if not sha1_re.search(id):
            error_now = ('You should be querying for a SHA1.  These are written in '
                         'hexadecimal (so they consist of a-f and 0-9).  Your query, '
                         '%s, is hence not a possible prefix.' % id)
        else:
            error_now = None
        id = id.lower()

        try:
            page = max(int(request.params.get('page', 0)), 1)
        except ValueError:
            page = 1

        try:
            limit = min(int(limit or request.params.get('limit', 10)), 50)
        except ValueError:
            limit = 10
        offset = (page - 1) * limit
        matching, count = models.GitObject.lookup_by_sha1(sha1=id,
                                                          partial=True,
                                                          limit=limit)
        if count > 1:
            matching.skip(offset)
        c.page = page
        c.start = offset + 1
        c.end = min(page * limit, count)
        c.limit = limit
        c.objects = matching
        c.count = count
        c.queried_id = id
        # Nonsensical if count == 0
        c.out_of_range = c.start > count
        return render('query.mako', controller='query', error_now=error_now)

    def query_with_string(self):
        query = request.params.get('query', '')
        limit = request.params.get('limit', '')
        redirect_to(action='query', id=query, limit=limit)
    q = query
