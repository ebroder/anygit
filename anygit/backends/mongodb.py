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
save_classes = []
collection_to_class = {}

## Exported functions

def create_schema():
    # Clear out the database
    for klass in collection_to_class.itervalues():
        klass._object_store.remove()

    # Set up indexes
    Repository._object_store.ensure_index('url')
    Repository._object_store.ensure_index('approved')
    Repository._object_store.ensure_index('count')

def init_model(connection):
    """Call me before using any of the tables or classes in the model."""
    raw_db = connection.anygit
    db = connection.anygit
    # Transform
    db.add_son_manipulator(TransformObject())

    for obj in globals().itervalues():
        if type(obj) == type and issubclass(obj, MongoDbModel) and hasattr(obj, '__tablename__'):
            save_classes.append(obj)
            tablename = getattr(obj, '__tablename__')
            obj._object_store = getattr(db, tablename)
            obj._raw_object_store = getattr(raw_db, tablename)
            collection_to_class[obj._object_store] = obj

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
    for klass in save_classes:
        if klass._save_list:
            logger.debug('Saving %d %s instances...' % (len(klass._save_list), klass.__name__))

        for instance in klass._save_list:
            try:
                updates = instance.get_updates()
                klass._object_store.update({'_id' : instance.id},
                                           updates,
                                           upsert=True)
            except:
                logger.critical('Had some trouble saving %s' % instance)
                raise
            instance.mark_saved()
            instance.new = False
            instance._pending_save = False
            instance._changed = False
            instance._pending_updates.clear()
        klass._save_list = klass._save_list[0:0]
        klass._cache.clear()
    logger.debug('Commit complete.')

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
    elif isinstance(db_object, basestring):
        return db_object
    else:
        raise exceptions.Error('Illegal type %s (instance %r)' % (type(db_object), db_object))

def canonicalize_to_object(id, cls=None):
    if not cls:
        cls = GitObject
    if isinstance(id, basestring):
        obj = cls.get(id=id)
    elif isinstance(id, cls):
        obj = id
        id = obj.id
    else:
        raise exceptions.Error('Illegal type %s (instance %r)' % (type(id), id))
    return id, obj

def sanitize_unicode(u):
    if isinstance(u, str):
        try:
            return unicode(u, 'utf-8')
        except UnicodeDecodeError:
            sanitized = unicode(u, 'iso-8859-1')
            logger.info('Invalid unicode detected: %r.  Assuming iso-8859-1 (%s)' % (u, sanitized))
            return sanitized
    else:
        return u

def convert_iterable(target, dest):
    if not hasattr(target, '__iter__'):
        return target
    elif not isinstance(target, dest):
        return dest(target)

def make_persistent_set():
    # TODO: transparently diff and persist this.
    backend_attr = '__%s' % hex(random.getrandbits(128))
    def _getter(self):
        if not hasattr(self, backend_attr):
            setattr(self, backend_attr, set())
        return getattr(self, backend_attr)
    def _setter(self, value):
        value = set(convert_iterable(entry, tuple) for entry in value)
        setattr(self, backend_attr, value)
    return property(_getter, _setter)

def make_persistent_attribute(name, default=None):
    backend_attr = '__%s' % hex(random.getrandbits(128))
    def _getter(self):
        if not hasattr(self, backend_attr):
            setattr(self, name, default)
        return getattr(self, backend_attr)
    def _setter(self, value):
        if hasattr(self, backend_attr) and value == getattr(self, backend_attr):
            return
        self._changed = True
        setting = self._pending_updates.setdefault('$set', {})
        setting[name] = value
        setattr(self, backend_attr, value)
    return property(_getter, _setter)

def rename_dict_keys(dict, to_backend=True):
    attrs = [('_id', 'id')]
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


class Error(Exception):
    pass


class AbstractMethodError(Exception):
    pass


class TransformObject(son_manipulator.SONManipulator):
    def transform_incoming(self, object, collection):
        """Transform an object heading for the database"""
        return object

    def transform_outgoing(self, son, collection):
        """Transform an object retrieved from the database"""
        if 'type' in son:
            klass = classify(son['type'])
            return klass.demongofy(son)
        else:
            try:
                return collection_to_class[collection].demongofy(son)
            except KeyError:
                return son

class Map(object):
    def __init__(self, result, fun, count=None):
        self.result = result
        self._iterator = (fun(i) for i in result)
        self._count = count

    def __iter__(self):
        return iter(self._iterator)

    def count(self):
        if self._count is None:
            self._count = self.result.count()
        return self._count

    def next(self):
        return self._iterator.next()
        

class MongoDbModel(object):
    # Should provide these in subclasses
    _cache = {}
    _save_list = None
    batched = True
    has_type = False

    # Attributes: id, type

    def __init__(self, _raw_dict={}, **kwargs):
        rename_dict_keys(kwargs, to_backend=True)
        self._pending_updates = {}
        self._init_from_dict(_raw_dict)
        self._pending_updates.clear()
        self._init_from_dict(kwargs)
        self.new = True
        self._pending_save = False
        self._changed = False

    def _init_from_dict(self, dict):
        rename_dict_keys(dict, to_backend=False)
        for k, v in dict.iteritems():
            if k == 'type':
                assert v == self.type
                continue
            setattr(self, k, v)

    def _set(self, attr, value):
        # TODO: I think that setattr on sets is a bit borked.  Maybe fix that.
        setter = self._pending_updates.setdefault('$set', {})
        setter[attr] = value

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

    @property
    def type(self):
        return type(self).__name__.lower()

    @classmethod
    def find(cls, kwargs):
        if cls.has_type:
            kwargs.setdefault('type', cls.__name__.lower())
        return cls._object_store.find(kwargs)

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
        results = cls.find(kwargs)
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
        return cls._object_store.find()

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
                self._save_list.append(self)
                if self._pending_save:
                    return
                self._pending_save = True
                if curr_transaction_window >= max_transaction_window:
                    flush()
                    curr_transaction_window = 0
                else:
                    curr_transaction_window += 1
            else:
                raise NotImplementedError('Non batched saves are not supported')
            return True
        else:
            return False

    def delete(self):
        raise NotImplementedError()

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

    def get_updates(self):
        # Hack to add *something* for new insertions
        if self.has_type:
            self._pending_updates.setdefault('$set', {}).setdefault('type', self.type)
        elif not self._pending_updates:
            # Doing an upsert requires a non-empty object, so put in something small
            self._pending_updates.setdefault('$set', {}).setdefault('d', 1)
        return self._pending_updates

    def mark_saved(self):
        self.new = False
        self._pending_save = False
        self._changed = False
        self._pending_updates.clear()

    def __str__(self):
        return '%s: %s' % (self.type, self.id)

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id


class GitObjectAssociation(MongoDbModel, common.CommonMixin):
    has_type = False
    key1_name = None
    key2_name = None

    def __init__(self, key1=None, key2=None, _raw_dict={}):
        super(GitObjectAssociation, self).__init__(_raw_dict=_raw_dict)
        if key1:
            setattr(self, self.key1_name, key1)
        if key2:
            setattr(self, self.key2_name, key2)
        if key1 and key2:
            self._id = key1 + key2

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        assert len(value) == 80
        setattr(self, self.key1_name, value[0:40])
        setattr(self, self.key2_name, value[40:80])

    @classmethod
    def get_all(cls, sha1):
        safe_sha1 = '^%s' % re.escape(sha1)
        return cls._object_store.find({'_id' : re.compile(safe_sha1)})

    def __str__(self):
        return '%s: %s=%s, %s=%s' % (self.type,
                                     self.key1_name, getattr(self, self.key1_name),
                                     self.key2_name, getattr(self, self.key2_name))


class BlobTree(GitObjectAssociation):
    __tablename__ = 'blob_trees'
    _save_list = []
    _cache = {}
    key1_name = 'blob_id'
    key2_name = 'tree_id'

    name = make_persistent_attribute('name')
    mode = make_persistent_attribute('mode')


class BlobTag(GitObjectAssociation):
    __tablename__ = 'blob_tags'
    _save_list = []
    _cache = {}
    key1_name = 'blob_id'
    key2_name = 'tag_id'


class TreeParentTree(GitObjectAssociation):
    __tablename__ = 'tree_parent_trees'
    _save_list = []
    _cache = {}
    key1_name = 'tree_id'
    key2_name = 'parent_tree_id'

    name = make_persistent_attribute('name')
    mode = make_persistent_attribute('mode')


class TreeCommit(GitObjectAssociation):
    __tablename__ = 'tree_commits'
    _save_list = []
    _cache = {}
    key1_name = 'tree_id'
    key2_name = 'commit_id'


class TreeTag(GitObjectAssociation):
    __tablename__ = 'tree_tags'
    _save_list = []
    _cache = {}
    key1_name = 'tree_id'
    key2_name = 'tag_id'


class CommitParentCommit(GitObjectAssociation):
    __tablename__ = 'commit_parent_commits'
    _save_list = []
    _cache = {}
    key1_name = 'commit_id'
    key2_name = 'parent_commit_id'


class CommitTree(GitObjectAssociation):
    __tablename__ = 'commit_trees'
    _save_list = []
    _cache = {}
    key1_name = 'commit_id'
    key2_name = 'tree_id'

    name = make_persistent_attribute('name')
    mode = make_persistent_attribute('mode')


class CommitTag(GitObjectAssociation):
    __tablename__ = 'commit_tags'
    _save_list = []
    _cache = {}
    key1_name = 'commit_id'
    key2_name = 'tag_id'


class TagParentTag(GitObjectAssociation):
    __tablename__ = 'tag_parent_tags'
    _save_list = []
    _cache = {}
    key1_name = 'tag_id'
    key2_name = 'parent_tag_id'


class GitObject(MongoDbModel, common.CommonGitObjectMixin):
    """The base class for git objects (such as blobs, commits, etc..)."""
    # Attributes: repository_ids, tag_ids, dirty
    __tablename__ = 'git_objects'
    has_type = True
    _save_list = []
    _cache = {}
    _repository_ids = make_persistent_set()
    dirty = make_persistent_attribute('dirty')

    @classmethod
    def lookup_by_sha1(cls, sha1, partial=False, offset=0, limit=10):
        # TODO: might want to disable lookup for dirty objects, or something
        if partial:
            safe_sha1 = '^%s' % re.escape(sha1)
            results = cls._object_store.find({'_id' : re.compile(safe_sha1)})
        else:
            results = cls._object_store.find({'_id' : sha1})
        count = results.count()
        return results.skip(offset).limit(limit), count

    @classmethod
    def all(cls):
        if cls == GitObject:
            return cls._object_store.find()
        else:
            return cls._object_store.find({'type' : cls.__name__.lower()})

    def mark_dirty(self, value):
        self.dirty = value

    @property
    def repository_ids(self):
        return Map(self._repository_ids, lambda x: x, count=len(self._repository_ids))    

    @property
    def repositories(self):
        return Repository.find_matching(self.repository_ids)

    def add_tag(self, tag_id):
        raise AbstractMethodError()

    @property
    def tags(self):
        return Tag.find_matching(self.tag_ids)

    def add_repository(self, repository_id, recursive=False):
        repository_id = canonicalize_to_id(repository_id)
        self._add_to_set('_repository_ids', repository_id)


class Blob(GitObject, common.CommonBlobMixin):
    """Represents a git Blob.  Has an id (the sha1 that identifies this
    object)"""

    def add_parent(self, parent_id, name, mode):
        name = sanitize_unicode(name)
        parent_id = canonicalize_to_id(parent_id)
        b = BlobTree(key1=self.id, key2=parent_id)
        b.name = name
        b.mode = mode
        b.save()

    @property
    def parent_ids_with_names(self):
        return Map(BlobTree.get_all(self.id), lambda bt: (bt.tree_id, bt.name))

    @property
    def parent_ids(self):
        return Map(self.parent_ids_with_names, lambda (id, name): id)

    @property
    def names(self):
        s = set(name for (id, name) in self.parent_ids_with_names)
        return Map(s, lambda x: x, count=len(s))

    @property
    def parents(self):
        return Tree.find_matching(self.parent_ids)

    @property
    def parents_with_names(self):
        return Map(self.parent_ids_with_names, lambda (id, name): (Tree.get(id), name))

    def add_tag(self, tag_id):
        tag_id = canonicalize_to_id(tag_id)
        b = BlobTag(key1=self.id, key2=tag_id)
        b.save()


### Still working here.

class Tree(GitObject, common.CommonTreeMixin):
    """Represents a git Tree.  Has an id (the sha1 that identifies this
    object)"""

    def add_parent(self, parent_id, name, mode):
        """Give this tree a parent.  Also updates the parent to know
        about this tree."""
        name = sanitize_unicode(name)
        parent_id = canonicalize_to_id(parent_id)
        b = TreeParentTree(key1=self.id, key2=parent_id)
        b.name = name
        b.mode = mode
        b.save()

    @property
    def parent_ids_with_names(self):
        return Map(TreeParentTree.get_all(self.id), lambda tpt: (tpt.tree_id, tpt.name))

    def add_commit(self, commit_id):
        commit_id = canonicalize_to_id(commit_id)
        t = TreeCommit(key1=self.id, key2=commit_id)
        t.save()

    @property
    def commit_ids(self):
        return Map(TreeCommit.get_all(self.id), lambda tc: tc.commit_id)

    @property
    def commits(self):
        return Commit.find_matching(self.commit_ids)

    @property
    def parent_ids(self):
        return Map(self.parent_ids_with_names, lambda (id, name): id)

    @property
    def names(self):
        return Map(self.parent_ids_with_names, lambda (id, name): name)

    @property
    def parents(self):
        return Tree.find_matching(self.parent_ids)

    @property
    def parents_with_names(self):
        return Map(self.parent_ids_with_names, lambda (id, name): (Tree.get(id), name))

    def add_tag(self, tag_id):
        tag_id = canonicalize_to_id(tag_id)
        b = TreeTag(key1=self.id, key2=tag_id)
        b.save()


class Tag(GitObject, common.CommonTagMixin):
    """Represents a git Tree.  Has an id (the sha1 that identifies this
    object)"""
    object_id = make_persistent_attribute('object_id')

    def add_tag(self, tag_id):
        tag_id = canonicalize_to_id(tag_id)
        b = TagParentTag(key1=self.id, key2=tag_id)
        b.save()

    def set_object_id(self, object_id):
        object_id = canonicalize_to_id(object_id)
        self.object_id = object_id

    @property
    def object(self):
        return GitObject.get(id=self.object_id)


class Commit(GitObject, common.CommonCommitMixin):
    """Represents a git Commit.  Has an id (the sha1 that identifies
    this object).  Also contains blobs, trees, and tags."""
    parent_ids = make_persistent_set()

    def add_parent(self, parent):
        self.add_parents([parent])

    def add_parents(self, parent_ids):
        parent_ids = set(canonicalize_to_id(p) for p in parent_ids)
        self._add_all_to_set('parent_ids', parent_ids)

    def add_as_submodule_of(self, tree_id, name, mode):
        tree_id = canonicalize_to_id(tree_id)
        name = sanitize_unicode(name)
        b = CommitTree(key1=self.id, key2=tree_id)
        b.name = name
        b.mode = mode
        b.save()

    @property
    def submodule_of_with_names(self):
        return Map(CommitTree.get_all(self.id), lambda ct: (ct.tree_id, ct.name))

    @property
    def submodule_of(self):
        return Map(self.submodule_of_with_names, lambda (id, name): id)

    @property
    def parents(self):
        return Commit.find_matching(self.parent_ids)

    def add_tag(self, tag_id):
        tag_id = canonicalize_to_id(tag_id)
        b = CommitTag(key1=self.id, key2=tag_id)
        b.save()


class Repository(MongoDbModel, common.CommonRepositoryMixin):
    """A git repository.  Contains many commits."""
    _save_list = []
    __tablename__ = 'repositories'

    # Attributes: url, last_index, indexing, commit_ids
    url = make_persistent_attribute('url')
    last_index = make_persistent_attribute('last_index', default=datetime.datetime(1970,1,1))
    indexing = make_persistent_attribute('indexing', default=False)
    remote_heads = make_persistent_set()
    new_remote_heads = make_persistent_set()
    been_indexed = make_persistent_attribute('been_indexed', default=False)
    approved = make_persistent_attribute('approved', default=False)
    count = make_persistent_attribute('count', default=0)

    def __init__(self, *args, **kwargs):
        super(Repository, self).__init__(*args, **kwargs)
        # Hack to default this, thus persisting it.
        self.indexing

    @classmethod
    def get_indexed_before(cls, date):
        """Get all repos indexed before the given date and not currently
        being indexed."""
        # Hack: should be lazier about this.
        if date is not None:
            return cls._object_store.find({'last_index' : {'$lt' : date},
                                           'indexing' : False,
                                           'approved' : True})
        else:
            return cls._object_store.find({'approved' : True,
                                           'indexing' : False})

    @classmethod
    def get_by_highest_count(cls, n=None, descending=True):
        if descending:
            order = pymongo.DESCENDING
        else:
            order = pymongo.ASCENDING
        base = cls._object_store.find().sort('count', order)
        if n:
            full = base.limit(n)
        else:
            full = base
        return full

    def set_count(self, value):
        self.count = value

    def count_objects(self):
        return GitObject._object_store.find({'repository_ids' : self.id}).count()

    def set_new_remote_heads(self, new_remote_heads):
        self._set('new_remote_heads', list(self.new_remote_heads))

    def set_remote_heads(self, remote_heads):
        self._set('remote_heads', list(self.new_remote_heads))

    def __str__(self):
        return 'Repository: %s' % self.url


class Aggregate(MongoDbModel, common.CommonMixin):
    """Singleton class that contains aggregate data about the indexer"""
    __tablename__ = 'aggregate'
    instance = None
    _save_list = []

    indexed_repository_count = make_persistent_attribute('indexed_repository_count', default=0)
    blob_count = make_persistent_attribute('blob_count', default=0)
    tree_count = make_persistent_attribute('tree_count', default=0)
    commit_count = make_persistent_attribute('commit_count', default=0)
    tag_count = make_persistent_attribute('tag_count', default=0)

    class index_executor(object):
        def __init__(self, klass, field):
            self.klass = klass
            self.field = field

        def __enter__(self):
            self.klass._object_store.ensure_index(self.field)
        
        def __exit__(self, type, value, traceback):
            for name, value in self.klass._object_store.index_information().iteritems():
                try:
                    if len(value) == 1 and isinstance(value, list) and value[0][0] == self.field:
                        self.klass._object_store.drop_index(name)
                        break
                except TypeError:
                    logger.error('Unexpected index information output %s for' %
                                 (value, self.field, self.klass.__name__))
            else:
                logger.error('No index on %s found for %s' % (self.field, self.klass.__name__))

    @classmethod
    def get(cls):
        if not cls.instance:
            try:
                cls.instance = super(Aggregate, cls).get(id='main')
            except exceptions.DoesNotExist:
                cls.instance = cls.create(id='main')
                flush()
        return cls.instance

    def refresh_all_counts(self):
        with Aggregate.index_executor(GitObject, 'repository_ids'):
            for repo in Repository.all():
                count = repo.count_objects()
                repo.set_count(count)
                logger.info('Setting count for %s to %d' % (repo, count))
                repo.save()

        with Aggregate.index_executor(Repository, 'been_indexed'):
            count = self.indexed_repository_count = Repository.find({'been_indexed' : True}).count()
            logger.info('Looks like there are %d indexed repositories' % count)
        self.save()
        flush()

        with Aggregate.index_executor(GitObject, 'type'):
            self.blob_count = Blob.find({}).count()
            self.tree_count = Tree.find({}).count()
            self.commit_count = Commit.find({}).count()
            self.tag_count = Tag.find({}).count()
            logger.info('Also, there are %d blobs, %d trees, %d commits, and %d tags' %
                        (self.blob_count, self.tree_count, self.commit_count, self.tag_count))
        self.save()
        flush()
