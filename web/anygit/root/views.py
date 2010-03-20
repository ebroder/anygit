from django import http
from django import shortcuts
import mako.template

from anygit import data
from anygit import settings
import anygit.data.models

def respond(view):
    def newview(request, *args, **kwargs):
        params = view(request, *args, **kwargs) or {}
        params.setdefault('__template__', 'root/%s.mako' % view.__name__)
        template = settings.MAKO_LOOKUP.get_template(params['__template__'])
        template_with_lookup = mako.template.Template(template.source,
                                                      lookup=settings.MAKO_LOOKUP)
        print params
        rendered_string = template_with_lookup.render(**params)
        return http.HttpResponse(rendered_string)
    return newview

@respond
def index(request):
    return {}

@respond
def query(request, query):
    result = {}
    objects = data.models.GitObject.lookup_by_sha1(sha1=query, partial=True)
    return {'objects' : objects}
