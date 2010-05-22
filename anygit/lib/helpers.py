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
repo_or_cz_re = re.compile(r'^(?:git|http)://repo.or.cz/([^/]*)$')

def get_url(obj):
    return util.url_for(controller='query', action='query', id=obj.id)

def _github_handle(repo, obj, match):
    values = {'user' : match.group(1),
              'repo' : match.group(2),
              'sha1' : obj.id,
              'type' : obj.type}
    if obj.type == 'commit':
        return 'http://github.com/%(user)s/%(repo)s/commit/%(sha1)s' % values
    elif obj.type == 'tree' or obj.type == 'blob':
        path_pair = obj.get_path(repo)
        # Could be in the middle of indexing
        if not path_pair:
            return None
        commit, path = path_pair
        values['path'] = path
        values['commit_sha1'] = commit.id
        return 'http://github.com/%(user)s/%(repo)s/%(type)s/%(commit_sha1)s/%(path)s' % values
    elif obj.type == 'tag':
        return get_view_url_for(repo, obj.object)

def _repo_or_cz_handle(repo, obj, match):
    values = {'repo' : match.group(1),
              'type' : obj.type,
              'sha1' : obj.id}
    return 'http://repo.or.cz/w/%(repo)s/%(type)s/%(sha1)s' % values

def get_view_url_for(repo, obj):
    match = github_re.search(repo.url)
    if match:
        return _github_handle(repo, obj, match)
    match = repo_or_cz_re.search(repo.url)
    if match:
        return _repo_or_cz_handle(repo, obj, match)
    
def pluralize(number, singular, plural=None):
    if plural is None:
        plural = '%ss' % singular
    if number == 1:
        return '1 %s' % singular
    else:
        return '%d %s' % (number, plural)
