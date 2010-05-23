"""Helper functions

Consists of functions to typically be used within templates, but also
available to Controllers. This module is available to templates as 'h'.
"""

import re
from routes import util
from webhelpers.pylonslib import Flash as _Flash
flash = _Flash('flash')
error = _Flash('error')

github_com_re = re.compile(r'^(?:git|http)://github\.com/([^/]*)/([^/]*)\.git$')
git_kernel_org_re = re.compile(r'^(?:git|http)://git\.kernel\.org/pub/scm/([^/]*)/(.*\.git$)')
repo_or_cz_re = re.compile(r'^(?:git|http)://repo\.or\.cz/([^/]*)$')
perl5_git_perl_org_re = re.compile(r'^(?:git|http)://perl5\.git\.perl\.org/([^/]*)$')
git_gnome_org_re = re.compile(r'^(?:git|http)://git\.gnome\.org/([^/]*)$')
cgit_freedesktop_org_re = re.compile(r'^(?:git|http)://anongit\.freedesktop\.org/(.*)$')

def get_url(obj):
    return util.url_for(controller='query', action='query', id=obj.id)

def github_com_handle(repo, obj, match):
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

def git_kernel_org_handle(repo, obj, match):
    values = {'user' : match.group(1),
              'suffix' : match.group(2),
              'type' : obj.type,
              'sha1' : obj.id}
    return 'http://git.kernel.org/?p=%(user)s/%(suffix)s;a=%(type)s;h=%(sha1)s' % values

def repo_or_cz_handle(repo, obj, match):
    values = {'repo' : match.group(1),
              'type' : obj.type,
              'sha1' : obj.id}
    return 'http://repo.or.cz/w/%(repo)s/%(type)s/%(sha1)s' % values

def perl5_git_perl_org_handle(repo, obj, match):
    values = {'extension' : match.group(1),
              'type' : obj.type,
              'sha1' : obj.id}
    if obj.type == 'commit' or obj.type == 'tree':
        return 'http://perl5.git.perl.org/%(extension)s/%(type)s/%(sha1)s' % values
    else:
        parent_and_name = obj.get_path(repo, recursive=False)
        if not parent_and_name:
            return None
        values['parent_id'] = parent_and_name[0].id
        values['name'] = parent_and_name[1]
        return 'http://perl5.git.perl.org/%(extension)s/%(type)s/%(parent_id)s:/%(name)s' % values

def _cgit_handle(base, repo, obj, match):
    values = {'base' : base,
              'extension' : match.group(1),
              'sha1' : obj.id}
    if obj.type == 'commit':
        return '%(base)s/%(extension)s/commit/?id=%(sha1)s' % values
    else:
        parent_and_path = obj.get_path(repo)
        if not parent_and_path:
            return None
        values['parent_id'] = parent_and_path[0].id
        values['path'] = parent_and_path[1]
        return ('%(base)s/%(extension)s/tree/%(path)s?id=%(parent_id)s' % values)

def git_gnome_org_handle(repo, obj, match):
    return _cgit_handle('http://git.gnome.org/browse', repo, obj, match)

def cgit_freedesktop_org_handle(repo, obj, match):
    return _cgit_handle('http://cgit.freedesktop.org', repo, obj, match)

def get_view_url_for(repo, obj):
    for regex, handler in [(github_com_re, github_com_handle),
                           (git_kernel_org_re, git_kernel_org_handle),
                           (repo_or_cz_re, repo_or_cz_handle),
                           (perl5_git_perl_org_re, perl5_git_perl_org_handle),
                           (git_gnome_org_re, git_gnome_org_handle),
                           (cgit_freedesktop_org_re, cgit_freedesktop_org_handle)]:
        match = regex.search(repo.url)
        if match:
            return handler(repo, obj, match)
    
def pluralize(number, singular, plural=None):
    if plural is None:
        plural = '%ss' % singular
    if number == 1:
        return '1 %s' % singular
    else:
        return '%d %s' % (number, plural)

def liststyled(items, separator, style_left=None, style_right=None):
    if style_left is None:
        style_left=""
    if style_right is None:
        style_right=""
    styled_objects = map((lambda x: style_left + x + style_right), items)
    return separator.join(styled_objects)
