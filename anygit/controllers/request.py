import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from anygit.lib.base import BaseController, render
from anygit.data import exceptions
from anygit import models

log = logging.getLogger(__name__)

class RequestController(BaseController):
    def index(self):
        return render('/request/index.mako', controller='request')

    def do_request(self):
        if not models.Repository.exists(url=request.params['url']):
            models.Repository.create(url=request.params['url'])
            models.flush()
