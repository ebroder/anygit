"""
The data models
"""
import logging
import re
from django.conf import settings
domain = settings.CON.get_domain('objects')

def canon_val(val):
    """Canonicalize a value to a list"""
    if not isinstance(val, list):
        val = [val]
    return val

def escape(value):
    """Escape a value for use in a query."""
    # TODO: make this escape rather than vomit
    if not re.search('^[a-eA-E0-9]$', value):
        raise ValidationError('Invalid value')
    return value

def attribute(name):
    """Make 'name' into an accessible attribute"""
    def getter(self):
        return self._attributes[name]
    def setter(self, value):
        self._attributes[name] = canon_val(value)
    return property(getter, setter)

class Error(Exception):
    pass

class ValidationError(Error):
    pass

class Repository(object):
    def __init__(self, base_url, identifier):
        self.base_url = base_url
        self.identifier = identifier

class GitObject(object):
    _required_attributes = ['sha1']
    sha1 = attribute('sha1')

    def __init__(self, boto_object=None, **kwargs):
        self.boto_object = boto_object
        self._attributes = {}
        self._attributes['__type__'] = type(self).__name__
        for key, value in kwargs.iteritems():
            self._attributes[key] = canon_val(value)

    @classmethod
    def lookup_by_sha1(cls, sha1, partial=False):
        safe_sha1 = escape(sha1)
        if partial:
            result = domain.select('select * from objects where sha1'
                                   ' LIKE "%s%%"' %
                                   safe_sha1)
        else:
            result = domain.select('select * from objects where sha1="%s"' %
                                   safe_sha1)
        return GitObject.result2objects(result)

    @classmethod
    def result2objects(cls, result):
        objects = []
        for object in result:
            if '__type__' not in object:
                logging.error('Invalid object %s (no __type__ attribute)' %
                              object)
                continue
            t = object['__type__']
            if t == 'Blob':
                c = Blob
            elif t == 'Tree':
                c = Tree
            elif t == 'Commit':
                c = Commit
            else:
                raise ValueError('Invalid __type__ for %s' % object)
            instance = c(boto_object=object)
            objects.append(instance)
        return objects

    def validate(self):
        if not self.sha1:
            raise ValidationError('Must provide a sha1')
        for attr in self._required_attributes:
            if not self._attributes[attr]:
                raise ValidationError('Must provide a value for %s' % attr)

    def save(self):
        self.validate()
        if self.boto_object is None:
            self.boto_object = domain.new_item(self.sha1)
        for attr, value in self._attributes.iteritems():
            self.boto_object[attr] = value
        return self.boto_object.save()

    def __str__(self):
        attrs = ['%s=%s' % (k, v) for k, v in self._attributes.iteritems()
                 if k != '__type__']
        name = type(self).__name__
        if attrs:
            return '%s: %s' % (name, ', '.join(attrs))
        else:
            return '%s (empty)' % name

    def __repr__(self):
        return str(self)

class Blob(GitObject):
    _required_attributes = ['sha1', 'commits']
    commits = attribute('commits')

    @property
    def commits(self):
        return self._attributes['commits']

    @commits.setter
    def commits(self, value):
        self._attributes['commits'] = value

class Tree(GitObject):
    _required_attributes = ['sha1', 'commits']
    commits = attribute('commits')

    @property
    def commits(self):
        return self._attributes['commits']

    @commits.setter
    def commits(self, value):
        self._attributes['commits'] = value

class Commit(GitObject):
    _required_attributes = ['sha1', 'repositories']
    repositories = attribute('repositories')

    @property
    def repositories(self):
        return self._attributes['repositories']

    @repositories.setter
    def repositories(self, value):
        self._attributes['repositories'] = value
