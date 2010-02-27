from django import shortcuts
from django import template

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
