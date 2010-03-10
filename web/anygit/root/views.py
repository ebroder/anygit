from django import shortcuts
from django import template
from anygit import data
import anygit.data.models

def respond(view):
    def newview(request, *args, **kwargs):
        params = view(request, *args, **kwargs) or {}
        params.setdefault('__template__', 'root/%s.tmpl' % view.__name__)
        context = template.RequestContext(request, params)
        return shortcuts.render_to_response(params['__template__'],
                                            context_instance=context)
    return newview

@respond
def index(request):
    return {}

@respond
def query(request, query):
    result = {}
    objects = data.models.GitObject.lookup_by_sha1(sha1=query, partial=True)
    return {'objects' : objects}
