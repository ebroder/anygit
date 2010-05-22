import boto
import datetime
import logging
import pymongo
import random
import re
import subprocess

from anygit.backends import common
from anygit.data import exceptions

logger = logging.getLogger(__name__)

max_transaction_window = 1000
curr_transaction_window = 0
connection = None

sha1_re = re.compile('^[a-f0-9]*')

## Exported functions

def create_schema():
    connect()
    # Clear out the database
    for domain in ['git_objects', 'repositories']:
        logger.info('Attempting to delete %s' % domain)
        try:
            connection.delete_domain(domain)
            logger.info('Deleted %s domain' % domain)
        except:
            logger.info('No %s domain to delete' % domain) 
        try:
            connection.delete_domain(domain)
        except:
            pass
        logger.info('Creating %s' % domain)
        connection.create_domain(domain)
        logger.info('Created %s' % domain)

def init_model(connection):
    """Call me before using any of the tables or classes in the model."""
    GitObject._raw_object_store = connection.get_domain('git_objects')
    GitObject._object_store = DomainWrapper(GitObject._raw_object_store)
    Repository._raw_object_store = connection.get_domain('repositories')
    Repository._object_store = DomainWrapper(Repository._raw_object_store)

def connect():
    global connection
    connection = boto.connect_sdb()

def setup():
    """
    Sets up the database session
    """
    connect()
    init_model(connection)

def save_batch(klass, batch):
    # TODO: batch_put_attributes, in groups of 25.
    try:
        klass._raw_object_store.batch_put_attributes(batch, replace=False)
    except:
        logger.critical('Had some trouble saving %r' % batch)
        raise

def flush():
    logger.debug('Committing...')
    classes = [GitObject]
    for klass in classes:
        logger.debug('Saving %d %s instances...' % (len(klass._save_list), klass.__name__))
        batch = {}
        for instance in klass._save_list:
            updates = instance.get_updates()
            if not updates:
                continue
            batch[instance.id] = updates
            if len(batch) >= 25:
                save_batch(klass, batch)
                batch.clear()

        if len(batch):
            save_batch(klass, batch)

        for instance in klass._save_list:
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
        del connection

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

def make_persistent_attribute(default=None, extractor=None):
    backend_attr = '__%s' % hex(random.getrandbits(128))
    def _getter(self):
        if not hasattr(self, backend_attr):
            setattr(self, backend_attr, default)
        return getattr(self, backend_attr)
    def _setter(self, value):
        self._changed = True
        if extractor:
            value = extractor(value)
        setattr(self, backend_attr, value)
    return property(_getter, _setter)

def bool_extractor(b):
    if b == 'False':
        return False
    else:
        return b

def datetime_extractor(d):
    if isinstance(d, str) or isinstance(d, unicode):
        return datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
    else:
        return d

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


class Query(object):
    def __init__(self, domain, query):
        if isinstance(query, dict):
            items = []
            for k, v in query.iteritems():
                if isinstance(v, list):
                    items.append('%s IN %r' % (k, tuple(v)))
                elif isinstance(v, dict):
                    if '$lt' in v:
                        items.append('%s < %r' % (k, v['$lt']))
                    else:
                        raise ValueError('Unrecognized query modifier %s' % v)
                else:
                    items.append('%s = %r' % (k, str(v)))
            query = ' and '.join(items)
        self.query = query
        self.domain = domain
        self._iterator = iter(self.domain.select(self._get_select()))

    def _get_select(self):
        # TODO: select a subset of attributes
        if self.query:
            full_query = 'select * from %s where %s' % (self.domain.name, self.query)
        else:
            full_query = 'select * from %s' % self.domain.name
        logger.info('Queried for %s' % full_query)
        return full_query

    def _get_count(self):
        if self.query:
            full_query = 'select count(*) from %s where %s' % (self.domain.name, self.query)
        else:
            full_query = 'select count(*) from %s' % self.domain.name
        logger.info('Queried for %s' % full_query)
        return full_query

    def __iter__(self):
        return iter(self.transform_outgoing(i) for i in self._iterator)

    def count(self):
        return int(self.domain.select(self._get_count()).next()['Count'])

    def next(self):
        return self.transform_outgoing(self._iterator.next())

    def transform_outgoing(self, son):
        """Transform an object retrieved from the database"""
        if '__type__' in son:
            klass = classify(son['__type__'])
            return klass.demongofy(son)
        else:
            return son


class DomainWrapper(object):
    def __init__(self, domain):
        self.domain = domain

    def transform_incoming(self, object):
        """Transform an object heading for the database"""
        return object.mongofy()

    def find(self, kwargs=''):
        return Query(self.domain, kwargs)

    def find_one(self, kwargs):
        result = self.find(kwargs)
        return result.next()

    def find_prefix(self, attr, value):
        # TODO: Perhaps do actual escaping here
        if not re.search(value):
            raise ValueError('Invalid sha1 prefix %s' % value)
        return Query(self.domain, '%s LIKE "%s%%"' % (attr, value))

    def set(self, id, attributes):
        self.domain.put_attributes(id, attributes)


class MongoDbModel(object):
    # Should provide these in subclasses
    _cache = {}
    _object_store = None
    _raw_object_store = None
    _save_list = None
    batched = True

    # Attributes: id, type

    def __init__(self, _raw_dict={}, **kwargs):
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
        adding = self._pending_updates.setdefault(set_name, [])
        adding.extend(values)
        
    def _add_to_set(self, set_name, value):
        return self._add_all_to_set(set_name, set([value]))

    def _set(self, attr, value):
        self._pending_updates[attr] = value
        setattr(self, attr, value)

    @property
    def type(self):
        return type(self).__name__.lower()

    @classmethod
    def get(cls, id):
        """Get an item with the given primary key"""
        cached = cls.get_from_cache(id=id)
        if cached:
            return cached
        else:
            return cls.get_by_attributes(id=id)

    @classmethod
    def get_from_cache_or_new(cls, id):
        cached = cls.get_from_cache(id=id)
        if cached:
            return cached
        else:
            return cls(id=id)

    @classmethod
    def get_from_cache(cls, id):
        if cls._cache and id in cls._cache:
            return cls._cache[id]
        else:
            return None

    @classmethod
    def get_by_attributes(cls, **kwargs):
        rename_dict_keys(kwargs, to_backend=True)
        results = cls._object_store.find(kwargs)
        count = results.count()
        if count == 1:
            result = results.next()
            if cls != GitObject:
                try:
                    assert isinstance(result, cls)
                except AssertionError:
                    logger.critical('Corrupt data %s, should be a %s' % (result, cls.__name__))
                    raise
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
        new_object = self._object_store.find_one({'_id' : self.id})
        self._init_from_dict(new_object.mongofy())

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
                self._object_store.set(self.id, self.mongofy())
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
        if not ids:
            return set()
        kwargs.update({'_id' : list(ids)})
        return cls._object_store.find(kwargs)

    @classmethod
    def count(cls, **kwargs):
        """Find the number of objects that match the given criteria"""
        kwargs['__type__'] = cls.__name__.lower()
        return cls._object_store.find(kwargs).count()

    def get_updates(self):
        if self.new:
            # Hack to add *something* for new insertions
            self._pending_updates.setdefault('__type__', self.type)
            self._pending_updates.setdefault('_id', self.id)
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
    # Attributes: repository_ids, tag_ids, dirty
    _save_list = set()
    _cache = {}
    repository_ids = make_persistent_set()
    tag_ids = make_persistent_set()
    dirty = make_persistent_attribute(extractor=bool_extractor)

    def mongofy(self, mongo_object):
        super(GitObject, self).mongofy(mongo_object)
        mongo_object['dirty'] = self.dirty
        mongo_object['tag_ids'] = list(self.tag_ids)
        mongo_object['repository_ids'] = list(self.repository_ids)
        return mongo_object

    @classmethod
    def lookup_by_sha1(cls, sha1, partial=False, offset=0, limit=10):
        # TODO: might want to disable lookup for dirty objects, or something
        if partial:
            results = cls._object_store.find_prefix('_id', sha1)
        else:
            results = cls._object_store.find({'_id' : sha1})
        count = results.count()
        return results.skip(offset).limit(limit), count

    @classmethod
    def all(cls):
        if cls == GitObject:
            return cls._object_store.find()
        else:
            return super(GitObject, cls).all()

    def mark_dirty(self, value):
        self.dirty = value
        self._set('dirty', value)

    def mark_saved(self):
        self.new = False
        self._pending_save = False
        self._changed = False
        self._pending_updates.clear()

    @property
    def repositories(self):
        return Repository.find_matching(self.repository_ids)

    def add_tag(self, tag_id):
        tag_id = canonicalize_to_id(tag_id)
        self._add_to_set('tag_ids', tag_id)

    @property
    def tags(self):
        return Tag.find_matching(self.tag_ids)


class Blob(GitObject, common.CommonBlobMixin):
    """Represents a git Blob.  Has an id (the sha1 that identifies this
    object)"""
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
    def names(self):
        return set(name for (id, name) in self.parent_ids_with_names)

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
    def names(self):
        return set(name for (id, name) in self.parent_ids_with_names)

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
        if recursive:
            for obj in self.children:
                obj.add_repository(repository_id, recursive=True)
        self._add_to_set('repository_ids', repository_id)
        self.save()


class Tag(GitObject, common.CommonTagMixin):
    """Represents a git Tree.  Has an id (the sha1 that identifies this
    object)"""
    # Attributes: object_id, repository_ids
    object_id = make_persistent_attribute()

    def add_repository(self, repository_id, recursive=False):
        repository_id = canonicalize_to_id(repository_id)
        if repository_id in self.repository_ids:
            return
        if recursive:
            self.object.add_repository(repository_id, recursive=True)
        self._add_to_set('repository_ids', repository_id)
        self.save()

    def mongofy(self, mongo_object=None):
        if mongo_object is None:
            mongo_object = {}
        super(Tag, self).mongofy(mongo_object)
        mongo_object['object_id'] = self.object_id
        mongo_object['repository_ids'] = list(self.repository_ids)
        return mongo_object

    def set_object(self, o_id):
        o_id = canonicalize_to_id(o_id)
        self._set('object_id', o_id)

    @property
    def object(self):
        return GitObject.get(self.object_id)


class Commit(GitObject, common.CommonCommitMixin):
    """Represents a git Commit.  Has an id (the sha1 that identifies
    this object).  Also contains blobs, trees, and tags."""
    # tree_id, parent_ids, submodule_of_with_names
    tree_id = make_persistent_attribute()
    parent_ids = make_persistent_set()
    submodule_of_with_names = make_persistent_set()

    def add_repository(self, repository_id, recursive=False):
        repository_id = canonicalize_to_id(repository_id)
        if repository_id in self.repository_ids:
            return
        if recursive:
            for parent in self.parents:
                parent.add_repository(repository_id, recursive=True)
            self.tree.add_repository(repository_id, recursive=True)
        self._add_to_set('repository_ids', repository_id)
        self.save()

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

    # Attributes: url, last_index, indexing, commit_ids
    # TODO: get correct types
    url = make_persistent_attribute()
    last_index = make_persistent_attribute(default=datetime.datetime(1970,1,1),
                                           extractor=datetime_extractor)
    indexing = make_persistent_attribute(default=False, extractor=bool_extractor)
    commit_ids = make_persistent_set()
    been_indexed = make_persistent_attribute(default=False, extractor=bool_extractor)
    approved = make_persistent_attribute(default=False, extractor=bool_extractor)

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
