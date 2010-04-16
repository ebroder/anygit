import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from anygit.lib.base import BaseController, render
from anygit import models

log = logging.getLogger(__name__)

class QueryController(BaseController):
    def index(self):
        return render('/index.mako', controller='query')

    def query(self, id):
        c.objects = models.GitObject.lookup_by_sha1(sha1=id, partial=True)
        return render('/query.mako', controller='query')
    q = query
