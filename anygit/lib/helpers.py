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
                  'sha1' : obj.id,
                  'type' : obj.type}
        if obj.type == 'commit':
            return 'http://github.com/%(user)s/%(repo)s/commit/%(sha1)s' % values
        elif obj.type == 'tree' or obj.type == 'blob':
            commit, path = obj.get_path(repo)
            values['path'] = path
            values['commit_sha1'] = commit.id
            return 'http://github.com/%(user)s/%(repo)s/%(type)s/%(commit_sha1)s/%(path)s' % values
        elif obj.type == 'tag':
            return get_view_url_for(repo, obj.object)
            
    
def pluralize(number, singular, plural=None):
    if plural is None:
        plural = '%ss' % singular
    if number == 1:
        return '1 %s' % singular
    else:
        return '%d %s' % (number, plural)
