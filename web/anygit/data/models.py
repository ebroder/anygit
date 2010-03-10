"""
The data models
"""
import logging
import re
from django.conf import settings

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

class SimpleDbModel(object):
    _pk_name = 'id'

    def __init__(self, boto_object=None, **kwargs):
        self.boto_object = boto_object
        self._attributes = {}
        if boto_object:
            self._attributes.update(boto_object)
        self._attributes['__type__'] = type(self).__name__
        for key, value in kwargs.iteritems():
            self._attributes[key] = canon_val(value)

    @property
    def pk(self):
        assert(len(self._attributes[self._pk_name]) == 1)
        return self._attributes[self._pk_name][0]

    @pk.setter
    def pk(self, value):
        self._attributes[self._pk_name] = value

    @classmethod
    def get(cls, pk):
        """Get an item with the given primary key"""
        result = cls.domain.get_item(pk)
        if result:
            return self.result2object(result)
        else:
            return None

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
    domain = settings.CON.get_domain('repositories')
    _required_attributes = ['name', 'url']
    _pk_name = 'name'
    name = attribute('name')
    url = attribute('url')

class GitObject(SimpleDbModel):
    domain = settings.CON.get_domain('objects')
    _required_attributes = ['sha1']
    _pk_name = 'sha1'
    sha1 = attribute('sha1')

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
    commits = attribute('commits')

class Tree(GitObject):
    _required_attributes = ['sha1', 'commits']
    commits = attribute('commits')

class Commit(GitObject):
    _required_attributes = ['sha1', 'repositories']
    repositories = attribute('repositories')
