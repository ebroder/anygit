"""
The data models
"""
import logging
import re
from django.conf import settings

partial_sha1_re = re.compile('^[a-fA-F0-9]*$')

def canon_val(val):
    """Canonicalize a value to a list"""
    if not isinstance(val, list):
        val = [val]
    return val

def classify(string):
    """Convert a class name to the corresponding class"""
    mapping = {'Repository' : Repository,
               'Blob' : Blob,
               'Tree' : Tree,
               'Commit' : Commit}
    try:
        return mapping[string]
    except KeyError:
        raise ValueError('No matching class found for %s' % string)

def escape(value):
    """Escape a value for use in a query."""
    # TODO: make this escape rather than vomit
    if not partial_sha1_re.search(value):
        raise ValidationError('Invalid value')
    return value

def attribute(name, singleton=False):
    """Make 'name' into an accessible attribute"""
    def getter(self):
        value = self._attributes[name]
        if singleton:
            value = value[0]
        return value
    def setter(self, value):
        self._attributes[name] = canon_val(value)
    return property(getter, setter)

class Error(Exception):
    pass

class ValidationError(Error):
    pass

class NoSuchObject(Error):
    pass

class SimpleDbModel(object):
    _pk_name = 'id'
    cache = {}

    def __init__(self, boto_object=None, **kwargs):
        self.boto_object = boto_object
        self._attributes = {}
        if boto_object:
            for key, value in boto_object.iteritems():
                self._attributes[key] = canon_val(value)
        self._attributes['__type__'] = type(self).__name__
        for key, value in kwargs.iteritems():
            self._attributes[key] = canon_val(value)

    @property
    def type(self):
        return type(self).__name__.lower()

    @property
    def pk(self):
        assert(len(self._attributes[self._pk_name]) == 1)
        return self._attributes[self._pk_name][0]

    @pk.setter
    def pk(self, value):
        self._attributes[self._pk_name] = value

    @classmethod
    def get(cls, pk, silent=False):
        """Get an item with the given primary key"""
        if pk in cls.cache:
            return cls.cache[pk]
        result = cls.domain.get_item(pk)
        if result:
            result = cls.result2object(result)
            cls.cache[pk] = result
            return result
        else:
            if silent:
                return None
            else:
                raise NoSuchObject('No %s with primary key %s found' %
                                   (cls.__name__, pk))

    @classmethod
    def result2objects(cls, result):
        objects = []
        for object_data in result:
            try:
                object = cls.result2object(object_data)
            except (TypeError, KeyError):
                logging.exception('Could not convert object to result')
            else:
                objects.append(object)
        return objects

    @classmethod
    def result2object(cls, result):
        klass = classify(result['__type__'])
        instance = klass(boto_object=result)
        return instance

    @classmethod
    def all(cls):
        result = cls.domain.select('select * from %s' % cls.domain.name)
        return cls.result2objects(result)

    def validate(self):
        for attr in self._required_attributes:
            if attr not in self._attributes or not self._attributes[attr]:
                raise ValidationError('Must provide a value for %s' % attr)

    def save(self):
        self.validate()
        if self.boto_object is None:
            self.boto_object = self.domain.new_item(self.pk)
        for attr, value in self._attributes.iteritems():
            self.boto_object[attr] = value
        return self.boto_object.save()

    def delete(self):
        self.boto_object.delete()

    def __str__(self):
        attrs = ['%s=%s' % (k, v) for k, v in self._attributes.iteritems()
                 if k != '__type__']
        name = type(self).__name__
        if attrs:
            return '<%s: %s>' % (name, ', '.join(attrs))
        else:
            return '<%s (empty)>' % name

    def __repr__(self):
        return str(self)

class Repository(SimpleDbModel):
    cache = {}
    domain = settings.CON.get_domain('repositories')
    _required_attributes = ['name', 'url']
    _pk_name = 'name'
    name = attribute('name', singleton=True)
    url = attribute('url', singleton=True)

class GitObject(SimpleDbModel):
    cache = {}
    domain = settings.CON.get_domain('objects')
    _required_attributes = ['sha1']
    _pk_name = 'sha1'
    sha1 = attribute('sha1', singleton=True)

    @classmethod
    def lookup_by_sha1(cls, sha1, partial=False):
        safe_sha1 = escape(sha1)
        if partial:
            result = cls.domain.select('select * from objects where sha1'
                                   ' LIKE "%s%%"' %
                                   safe_sha1)
        else:
            result = cls.domain.select('select * from objects where '
                                        ' sha1="%s"' %
                                        safe_sha1)
        return cls.result2objects(result)

class Blob(GitObject):
    _required_attributes = ['sha1', 'commits']
    commits = attribute('commits', singleton=False)

    @property
    def repositories_friendly(self):
        commit_objects = [Commit.get(commit) for commit in self.commits]
        repo_names = sum([object.repositories_friendly
                          for object in commit_objects], [])
        return list(set(repo_names))

class Tree(GitObject):
    _required_attributes = ['sha1', 'commits']
    commits = attribute('commits', singleton=False)

class Commit(GitObject):
    _required_attributes = ['sha1', 'repositories']
    repositories = attribute('repositories', singleton=False)

    @property
    def repositories_friendly(self):
        return [Repository.get(repo).url for repo in self.repositories]

