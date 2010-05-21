import datetime
import logging
import pymongo
import random
import re
import subprocess

from pymongo import son_manipulator
from pylons import config

from anygit.backends import common
from anygit.data import exceptions

logger = logging.getLogger(__name__)

max_transaction_window = 1000
curr_transaction_window = 0
connection = None

## Exported functions

def create_schema():
    # Clear out the database
    GitObject._object_store.remove()
    Repository._object_store.remove()

def init_model(connection):
    """Call me before using any of the tables or classes in the model."""
    raw_db = connection.anygit

    db = connection.anygit
    # Transform
    db.add_son_manipulator(TransformObject())

    GitObject._object_store = db.git_objects
    GitObject._raw_object_store = raw_db.git_objects

    Repository._object_store = db.repositories
    Repository._raw_object_store = raw_db.repositories

def setup():
    """
    Sets up the database session
    """
    global connection
    port = config.get('mongodb.port', None)
    if port:
        port = int(port)
    connection = pymongo.Connection(config['mongodb.url'],
                                    port)
    init_model(connection)

def flush():
    logger.debug('Committing...')
    classes = [GitObject]
    for klass in classes:
        logger.debug('Saving %d objects for %s...' % (len(klass._save_list), klass.__name__))

        for instance in klass._save_list:
            klass._object_store.update({'_id' : instance.id},
                                       instance.get_updates(),
                                       upsert=True)
            instance.mark_saved()
            instance.new = False
            instance._pending_save = False
            instance._changed = False
            instance._pending_updates.clear()
        klass._save_list.clear()
        klass._cache.clear()
        logger.debug('Saving %s complete.' % klass.__name__)

def destroy_session():
    if connection is not None:
        connection.disconnect()

## Internal functions

def classify(string):
    """Convert a class name to the corresponding class"""
    mapping = {'repository' : Repository,
               'blob' : Blob,
               'tree' : Tree,
               'commit' : Commit,
               'tag' : Tag}
    try:
        return mapping[string]
    except KeyError:
        raise ValueError('No matching class found for %s' % string)

def canonicalize_to_id(db_object):
    if isinstance(db_object, MongoDbModel):
        return db_object.id
    elif isinstance(db_object, str) or isinstance(db_object, unicode):
        return db_object
    else:
        raise exceptions.Error('Illegal type %s (instance %r)' % (type(db_object), db_object))

def canonicalize_to_object(id, cls=None):
    if not cls:
        cls = GitObject
    if isinstance(id, str) or isinstance(id, unicode):
        obj = cls.get(id=id)
    elif isinstance(id, cls):
        obj = id
        id = obj.id
    else:
        raise exceptions.Error('Illegal type %s (instance %r)' % (type(id), id))
    return id, obj

def convert_iterable(target, dest):
    if not hasattr(target, '__iter__'):
        return target
    elif not isinstance(target, dest):
        return dest(target)

def make_persistent_set():
    backend_attr = '__%s' % hex(random.getrandbits(128))
    def _getter(self):
        if not hasattr(self, backend_attr):
            setattr(self, backend_attr, set())
        return getattr(self, backend_attr)
    def _setter(self, value):
        value = set(convert_iterable(entry, tuple) for entry in value)
        setattr(self, backend_attr, value)
    return property(_getter, _setter)

def make_persistent_attribute(default=None):
    backend_attr = '__%s' % hex(random.getrandbits(128))
    def _getter(self):
        if not hasattr(self, backend_attr):
            setattr(self, backend_attr, default)
        return getattr(self, backend_attr)
    def _setter(self, value):
        self._changed = True
        setattr(self, backend_attr, value)
    return property(_getter, _setter)

def rename_dict_keys(dict, to_backend=True):
    attrs = [('_id', 'id'), ('__type__', 'type')]
    if to_backend:
        for backend, frontend in attrs:
            if frontend in dict:
                dict[backend] = dict[frontend]
                del dict[frontend]
    else:
        for backend, frontend in attrs:
            if backend in dict:
                dict[frontend] = dict[backend]
                del dict[backend]

## Classes

class TransformObject(son_manipulator.SONManipulator):
    def transform_incoming(self, object, collection):
        """Transform an object heading for the database"""
        return object.mongofy()

    def transform_outgoing(self, son, collection):
        """Transform an object retrieved from the database"""
        if '__type__' in son:
            klass = classify(son['__type__'])
            return klass.demongofy(son)
        else:
            return son

class MongoDbModel(object):
    # Should provide these in subclasses
    _cache = {}
    _object_store = None
    _raw_object_store = None
    _save_list = None
    batched = True
    abstract = True

    # Attributes: id, type

    def __init__(self, _raw_dict={}, **kwargs):
        assert not self.abstract
        rename_dict_keys(kwargs, to_backend=True)
        kwargs.update(_raw_dict)
        self._init_from_dict(kwargs)
        self.new = True
        self._pending_updates = {}
        self._pending_save = False
        self._changed = False

    def _init_from_dict(self, dict):
        rename_dict_keys(dict, to_backend=False)
        for k, v in dict.iteritems():
            if k == 'type':
                assert v == self.type
                continue
            setattr(self, k, v)

    def _add_all_to_set(self, set_name, values):
        # TODO: to get the *right* semantics, should have a committed updates
        # and an uncommitted updates.
        assert isinstance(values, set)
        full_set = getattr(self, set_name)
        # Get rid of everything we already have
        values = values.difference(full_set)
        if not values:
            return
        full_set.update(values)
        adding = self._pending_updates.setdefault('$addToSet', {})
        target_set = adding.setdefault(set_name, {'$each' : []})
        target_set['$each'].extend(values)
        
    def _add_to_set(self, set_name, value):
        return self._add_all_to_set(set_name, set([value]))

    def _set(self, attr, value):
        setting = self._pending_updates.setdefault('$set', {})
        setting[attr] = value
        setattr(self, attr, value)

    @property
    def type(self):
        return type(self).__name__.lower()

    @classmethod
    def get(cls, id):
        """Get an item with the given primary key"""
        if cls._cache and id in cls._cache:
            return cls._cache[id]
        return cls.get_by_attributes(id=id)

    @classmethod
    def get_by_attributes(cls, **kwargs):
        rename_dict_keys(kwargs, to_backend=True)
        results = cls._object_store.find(kwargs)
        count = results.count()
        if count == 1:
            result = results.next()
            assert isinstance(result, cls)
            return result
        elif count == 0:
            raise exceptions.DoesNotExist('%s: %s' % (cls.__name__, kwargs))
        else:
            raise exceptions.NotUnique('%s: %s' % (cls.__name__, kwargs))

    @classmethod
    def all(cls):
        return cls._object_store.find({'__type__' : cls.__name__.lower()})

    @classmethod
    def exists(cls, **kwargs):
        rename_dict_keys(kwargs, to_backend=True)
        return cls._object_store.find(kwargs).count() > 0

    def refresh(self):
        dict = self._raw_object_store.find_one({'_id' : self.id})
        self._init_from_dict(dict)

    def validate(self):
        """A stub method.  Should be overriden in subclasses."""
        pass

    @property
    def changed(self):
        """Indicate whether this object is changed from the version in
        the database.  Returns True for new objects."""
        return self.new or self._changed or self._pending_updates

    def save(self):
        global curr_transaction_window
        self.validate()
        if not self._errors:
            if not (self.changed or self.new):
                return True
            elif self.batched:
                self._cache[self.id] = self
                self._save_list.add(self)
                if self._pending_save:
                    return
                self._pending_save = True
                if curr_transaction_window >= max_transaction_window:
                    flush()
                    curr_transaction_window = 0
                else:
                    curr_transaction_window += 1
            else:
                # TODO: don't have to clobber the whole object here...
                self._object_store.update({'_id' : self.id}, self.mongofy(), upsert=True)
            return True
        else:
            return False

    def delete(self):
        raise NotImplementedError()

    def mongofy(self, mongo_object):
        mongo_object['_id'] = self.id
        mongo_object['__type__'] = self.type
        return mongo_object

    @classmethod
    def demongofy(cls, son):
        if '_id' in son:
            son['id'] = son['_id']
            del son['_id']
        elif 'id' not in son:
            raise exceptions.ValidationError('Missing attribute id in %s' % son)
        instance = cls(_raw_dict=son)
        instance.new = False
        return instance

    @classmethod
    def find_matching(cls, ids, **kwargs):
        """Given a list of ids, find the matching objects"""
        kwargs.update({'_id' : { '$in' : list(ids) }})
        return cls._object_store.find(kwargs)

    @classmethod
    def count(cls, **kwargs):
        """Find the number of objects that match the given criteria"""
        kwargs['__type__'] = cls.__name__.lower()
        return cls._object_store.find(kwargs).count()

    def get_updates(self):
        if self.new:
            # Hack to add *something* for new insertions
            self._pending_updates.setdefault('$set', {}).setdefault('__type__', self.type)
        return self._pending_updates

    def __str__(self):
        return '%s: %s' % (self.type, self.id)

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id


class GitObject(MongoDbModel, common.CommonGitObjectMixin):
    """The base class for git objects (such as blobs, commits, etc..)."""
    # Attributes: complete
    _save_list = set()
    _cache = {}
    repository_ids = make_persistent_set()

    complete = make_persistent_attribute()

    def mongofy(self, mongo_object):
        super(GitObject, self).mongofy(mongo_object)
        mongo_object['complete'] = self.complete
        mongo_object['repository_ids'] = list(self.repository_ids)
        return mongo_object

    @classmethod
    def lookup_by_sha1(cls, sha1, partial=False, offset=0, limit=10):
        if partial:
            safe_sha1 = '^%s' % re.escape(sha1)
            results = cls._object_store.find({'_id' : re.compile(safe_sha1),
                                              'complete' : True})
        else:
            results = cls._object_store.find({'_id' : sha1,
                                              'complete' : True})
        count = results.count()
        return results.skip(offset).limit(limit), count

    @classmethod
    def all(cls):
        if cls == GitObject:
            return cls._object_store.find()
        else:
            return super(GitObject, cls).all()

    def mark_complete(self):
        self.complete = True
        self._set('complete', True)

    def mark_saved(self):
        self.new = False
        self._pending_save = False
        self._changed = False
        self._pending_updates.clear()

    @property
    def repositories(self):
        return Repository.find_matching(self.repository_ids)


class Blob(GitObject, common.CommonBlobMixin):
    """Represents a git Blob.  Has an id (the sha1 that identifies this
    object)"""
    abstract = False
    # Attributes: parent_ids.
    parent_ids_with_names = make_persistent_set()

    def mongofy(self, mongo_object=None):
        if mongo_object is None:
            mongo_object = {}
        super(Blob, self).mongofy(mongo_object)
        mongo_object['parent_ids_with_names'] = [list(entry) for entry in self.parent_ids_with_names]
        return mongo_object

    def add_parent(self, parent_id, name):
        parent_id = canonicalize_to_id(parent_id)
        self._add_to_set('parent_ids_with_names', (parent_id, name))

    @property
    def parent_ids(self):
        return set(id for (id, name) in self.parent_ids_with_names)

    @property
    def parents(self):
        return Tree.find_matching(self.parent_ids)

    @property
    def parents_with_names(self):
        return set((Tree.get(id), name) for (id, name) in self.parent_ids_with_names)

    def add_repository(self, repository_id, recursive=False):
        repository_id = canonicalize_to_id(repository_id)
        self._add_to_set('repository_ids', repository_id)


class Tree(GitObject, common.CommonTreeMixin):
    """Represents a git Tree.  Has an id (the sha1 that identifies this
    object)"""
    abstract = False
    # Attributes: subtree_ids, blob_ids, parent_ids
    commit_ids = make_persistent_set()
    submodule_ids = make_persistent_set()
    parent_ids_with_names = make_persistent_set()
    children_ids = make_persistent_set()

    def add_parent(self, parent_id, name):
        """Give this tree a parent.  Also updates the parent to know
        about this tree."""
        parent_id = canonicalize_to_id(parent_id)
        self._add_to_set('parent_ids_with_names', (parent_id, name))

    def add_commit(self, commit_id):
        commit_id = canonicalize_to_id(commit_id)
        self._add_to_set('commit_ids', commit_id)

    def add_child(self, child_id):
        child_id = canonicalize_to_id(child_id)
        self._add_to_set('children_ids', child_id)

    def add_submodule(self, submodule_id):
        submodule_id = canonicalize_to_id(submodule_id)
        self._add_to_set('submodule_ids', submodule_id)

    @property
    def commits(self):
        return Commit.find_matching(self.commit_ids)

    @property
    def parent_ids(self):
        return set(id for (id, name) in self.parent_ids_with_names)

    @property
    def parents(self):
        return Tree.find_matching(self.parent_ids)

    @property
    def children(self):
        return GitObject.find_matching(self.children_ids)

    @property
    def submodules(self):
        return Commit.find_matching(self.submodule_ids)

    def mongofy(self, mongo_object=None):
        if mongo_object is None:
            mongo_object = {}
        super(Tree, self).mongofy(mongo_object)
        mongo_object['commit_ids'] = list(self.commit_ids)
        mongo_object['children_ids'] = list(self.children_ids)
        mongo_object['submodule_ids'] = list(self.submodule_ids)
        mongo_object['parent_ids_with_names'] = [list(entry) for entry in self.parent_ids_with_names]
        return mongo_object

    @property
    def parents_with_names(self):
        return set((Tree.get(id), name) for (id, name) in self.parent_ids_with_names)

    def add_repository(self, repository_id, recursive=False):
        repository_id = canonicalize_to_id(repository_id)
        if repository_id in self.repository_ids:
            return
        self._add_to_set('repository_ids', repository_id)
        if recursive:
            self.save()
            for obj in self.children:
                obj.add_repository(repository_id, recursive=True)


class Tag(GitObject, common.CommonTagMixin):
    """Represents a git Tree.  Has an id (the sha1 that identifies this
    object)"""
    abstract = False
    # Attributes: object_id, repository_ids
    object_id = make_persistent_attribute()

    def add_repository(self, repository_id, recursive=False):
        repository_id = canonicalize_to_id(repository_id)
        if repository_id in self.repository_ids:
            return
        self._add_to_set('repository_ids', repository_id)
        if recursive:
            self.save()
            self.object.add_repository(repository_id, recursive=True)

    def mongofy(self, mongo_object=None):
        if mongo_object is None:
            mongo_object = {}
        super(Tag, self).mongofy(mongo_object)
        mongo_object['object_id'] = self.object_id
        mongo_object['repository_ids'] = list(self.repository_ids)
        return mongo_object

    def set_object(self, o_id):
        o_id = canonicalize_to_id(o_id)
        self.object_id = o_id

    @property
    def object(self):
        return GitObject.get(self.object_id)


class Commit(GitObject, common.CommonCommitMixin):
    """Represents a git Commit.  Has an id (the sha1 that identifies
    this object).  Also contains blobs, trees, and tags."""
    abstract = False
    # tree_id, parent_ids, submodule_of_with_names
    tree_id = make_persistent_attribute()
    parent_ids = make_persistent_set()
    submodule_of_with_names = make_persistent_set()

    def add_repository(self, repository_id, recursive=False):
        repository_id = canonicalize_to_id(repository_id)
        if repository_id in self.repository_ids:
            return
        self._add_to_set('repository_ids', repository_id)
        if recursive:
            self.save()
            for parent in self.parents:
                parent.add_repository(repository_id, recursive=True)
            self.tree.add_repository(repository_id, recursive=True)

    def set_tree(self, tree_id):
        tree_id = canonicalize_to_id(tree_id)
        self._set('tree_id', tree_id)

    def add_parent(self, parent):
        self.add_parents([parent])

    def add_parents(self, parent_ids):
        parent_ids = set(canonicalize_to_id(p) for p in parent_ids)
        self._add_all_to_set('parent_ids', parent_ids)

    def add_as_submodule_of(self, repo_id, name):
        repo_id = canonicalize_to_id(repo_id)
        self._add_to_set('submodule_of_with_names', (repo_id, name))

    def mongofy(self, mongo_object=None):
        if mongo_object is None:
            mongo_object = {}
        super(Commit, self).mongofy(mongo_object)
        mongo_object['tree_id'] = self.tree_id
        mongo_object['parent_ids'] = list(self.parent_ids)
        mongo_object['submodule_of_with_names'] = [list(entry) for entry in self.submodule_of_with_names]
        return mongo_object

    @property
    def submodule_of(self):
        return set(id for (id, name) in self.submodule_of_with_names)

    @property
    def parents(self):
        return Commit.find_matching(self.parent_ids)

    @property
    def tree(self):
        return Tree.get(self.tree_id)


class Repository(MongoDbModel, common.CommonRepositoryMixin):
    """A git repository.  Contains many commits."""
    _save_list = set()
    batched = False
    abstract = False

    # Attributes: url, last_index, indexing, commit_ids
    url = make_persistent_attribute()
    last_index = make_persistent_attribute(default=datetime.datetime(1970,1,1))
    indexing = make_persistent_attribute(default=False)
    commit_ids = make_persistent_set()
    been_indexed = make_persistent_attribute(default=False)
    approved = make_persistent_attribute(default=False)

    def __init__(self, *args, **kwargs):
        super(Repository, self).__init__(*args, **kwargs)
        # TODO: persist this.
        if not hasattr(self, 'remote_heads'):
            self.remote_heads = {}

    def mongofy(self, mongo_object=None):
        if mongo_object is None:
            mongo_object = {}
        super(Repository, self).mongofy(mongo_object)
        mongo_object['url'] = self.url
        mongo_object['indexing'] = self.indexing
        mongo_object['last_index'] = self.last_index
        mongo_object['commit_ids'] = list(self.commit_ids)
        mongo_object['been_indexed'] = self.been_indexed
        mongo_object['approved'] = self.approved
        return mongo_object

    @classmethod
    def get_indexed_before(cls, date):
        """Get all repos indexed before the given date and not currently
        being indexed."""
        if date is not None:
            return cls._object_store.find({'last_index' : {'$lt' : date},
                                           'indexing' : False,
                                           'approved' : True})
        else:
            return cls._object_store.find({'indexing' : False,
                                           'approved' : True})

    def __str__(self):
        return 'Repository: %s' % self.url
