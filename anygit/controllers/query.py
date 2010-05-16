import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from anygit.lib.base import BaseController, render
from anygit import models

log = logging.getLogger(__name__)

class QueryController(BaseController):
    def index(self):
        return render('index.mako', controller='query')

    def query(self, id):
        matching, count = models.GitObject.lookup_by_sha1(sha1=id, partial=True)
        c.objects = matching
        c.count = count
        return render('query.mako', controller='query')

    def query_with_string(self):
        query = request.params.get('query', '')
        redirect_to(action='query', id=query)
    q = query
