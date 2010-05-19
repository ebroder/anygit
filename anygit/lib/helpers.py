"""Helper functions

Consists of functions to typically be used within templates, but also
available to Controllers. This module is available to templates as 'h'.
"""

import re
from routes import util
from webhelpers.pylonslib import Flash as _Flash
flash = _Flash('flash')
error = _Flash('error')

github_re = re.compile(r'^(?:git|http)://github.com/([^/]*)/([^/]*).git$')
github_mapper = {
    'commit' : 'http://github.com/%(user)s/%(repo)s/commit/%(sha1)s',
}


def get_url(obj):
    return util.url_for(controller='query', action='query', id=obj.id)

def get_view_url_for(repo, obj):
    match = github_re.search(repo.url)
    if match:
        values = {'user' : match.group(1),
                  'repo' : match.group(2),
                  'sha1' : obj.id}
        if obj.type in github_mapper:
            return github_mapper[obj.type] % values
    
def pluralize(number, singular, plural=None):
    if plural is None:
        plural = '%ss' % singular
    if number == 1:
        return '1 %s' % singular
    else:
        return '%d %s' % (number, plural)
