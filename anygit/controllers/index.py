import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from anygit.lib.base import BaseController, render
from anygit import models

log = logging.getLogger(__name__)

class IndexController(BaseController):
    def index(self):
        return render('index.mako', controller='index')
