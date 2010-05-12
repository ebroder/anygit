"""The base Controller API

Provides the BaseController class for subclassing.
"""
import os
from pylons.controllers import WSGIController
from pylons.templating import render_mako

from anygit import models

import webhelpers.html.secure_form
import webhelpers.html.tags

class BaseController(WSGIController):

    def __call__(self, environ, start_response):
        """Invoke the Controller"""
        # WSGIController.__call__ dispatches to the Controller method
        # the request is routed to. This routing information is
        # available in environ['pylons.routes_dict']
        try:
            return WSGIController.__call__(self, environ, start_response)
        finally:
            models.destroy_session()

def render(path, controller):
    return render_mako(os.path.join(controller, path), extra_vars={'webhelpers' : webhelpers})
