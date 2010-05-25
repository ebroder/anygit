import datetime
import logging
import MySQLdb
import random
import re
import subprocess

from pylons import config

from anygit.backends import common
from anygit.data import exceptions

logger = logging.getLogger(__name__)

max_transaction_window = 1000
curr_transaction_window = 0
connection = None
save_classes = []
collection_to_class = {}

sha1_re = re.compile('^[a-f0-9]*')

## Exported functions

def create_schema():
    print 'Huhh??'

def init_model(connection):
    """Call me before using any of the tables or classes in the model."""
    db = connection

    for obj in globals().itervalues():
        if type(obj) == type and issubclass(obj, MongoDbModel) and hasattr(obj, '__tablename__'):
            save_classes.append(obj)
            tablename = getattr(obj, '__tablename__')
            obj._object_store = Domain(db, tablename)
            collection_to_class[obj._object_store] = obj

def setup():
    """
    Sets up the database session
    """
    global connection
    connection = MySQLdb.connect(host=config.get('mysql.host'),
                                 user=config.get('mysql.user'),
                                 passwd=config.get('mysql.password'),
                                 db=config.get('mysql.db'))
    init_model(connection)

_count = 0
def flush():
    global _count
    _count += max_transaction_window
    logger.debug('Committing...')
    for klass in save_classes:
        if klass._save_list:
            logger.debug('Saving %d %s instances...' % (len(klass._save_list), klass.__name__))

        for instance in klass._save_list:
            try:
                updates = instance.get_updates()
                if not instance.new:
                    klass._object_store.update(instance.id,
                                               updates)
                else:
                    klass._object_store.insert(updates)
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

def make_persistent_attribute(name, default=None, extractor=None):
    backend_attr = '__%s' % hex(random.getrandbits(128))
    def _getter(self):
        if not hasattr(self, backend_attr):
            setattr(self, name, default)
        return getattr(self, backend_attr)
    def _setter(self, value):
        if hasattr(self, backend_attr) and value == getattr(self, backend_attr):
            return
        self._changed = True
        if extractor:
            value = extractor(value)
        self._pending_updates[name] = value
        setattr(self, backend_attr, value)
    return property(_getter, _setter)

def bool_extractor(b):
    if b == '0':
        return False
    elif b == '1':
        return True
    else:
        return b

def list_extractor(l):
    # Yeah yeah, no sanitization.  What are you going to do about it?
    return l.split(',')

def datetime_extractor(d):
    if isinstance(d, str) or isinstance(d, unicode):
        return datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
    else:
        return d

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


class Map(object):
    def __init__(self, result, fun, count=None):
        self.result = result
        self.fun = fun
        self._count = count
        self._iterator = (fun(i) for i in result)

    def __iter__(self):
        return iter(self._iterator)

    def count(self):
        if self._count is None:
            self._count = self.result.count()
        return self._count

    def next(self):
        return self._iterator.next()

    def limit(self, limit):
        return Map(self.result.limit(limit), self.fun, self._count)


class Query(object):
    def __init__(self, domain, query):
        self.domain = domain
        if isinstance(query, dict):
            items = []
            for k, v in query.iteritems():
                if isinstance(v, list):
                    items.append('`%s` IN (%s)' % (k, ','.join(self.domain._encode(val) for val in v)))
                elif isinstance(v, dict):
                    if '$lt' in v:
                        items.append('`%s` < %s' % (k, self.domain._encode(v['$lt'])))
                    elif '$in' in v:
                        if v['$in']:
                            items.append('`%s` IN (%s)' %
                                         (k, ','.join(self.domain._encode(val) for val in v['$in'])))
                        else:
                            items.append('1 = 0')
                    else:
                        raise ValueError('Unrecognized query modifier %s' % v)
                else:
                    items.append('`%s` = %s' % (k, self.domain._encode(v)))
            query = ' and '.join(items)
        self.query = query
        self._iterator = iter(self.domain.select(self._get_select()))

    def _get_select(self):
        # TODO: select a subset of attributes
        if self.query:
            full_query = 'select * from `%s` where %s' % (self.domain.name, self.query)
        else:
            full_query = 'select * from `%s`' % self.domain.name
        return full_query

    def _get_count(self):
        if self.query:
            full_query = 'select count(*) as count from `%s` where %s' % (self.domain.name, self.query)
        else:
            full_query = 'select count(*) from `%s`' % self.domain.name
        return full_query

    def __iter__(self):
        return iter(self.transform_outgoing(i) for i in self._iterator)

    def count(self):
        return int(self.domain.select(self._get_count()).next()['count'])

    def next(self):
        return self.transform_outgoing(self._iterator.next())

    def transform_outgoing(self, son):
        """Transform an object retrieved from the database"""
        if 'type' in son:
            klass = classify(son['type'])
            return klass.demongofy(son)
        else:
            try:
                return collection_to_class[self.domain].demongofy(son)
            except KeyError:
                return son


class Domain(object):
    def __init__(self, connection, name):
        self.connection = connection
        self.name = name

    def find(self, kwargs=''):
        return Query(self, kwargs)

    def find_one(self, kwargs):
        result = self.find(kwargs)
        return result.next()

    def find_prefix(self, attr, value):
        # TODO: Perhaps do actual escaping here
        if not sha1_re.search(value):
            raise ValueError('Invalid sha1 prefix %s' % value)
        return Query(self, '%s LIKE "%s%%"' % (attr, value))

    def _encode(self, value):
        if isinstance(value, bool):
            if value:
                return '1'
            else:
                return '0'
        else:
            return repr(str(value).lstrip('u'))

    def _prepare_params(self, id, attributes):
        keys = []
        values = []
        # TODO: escape
        if id is not None:
            keys.append('`_id`')
            values.append(self._encode(id))
        for k, v in attributes.iteritems():
            keys.append('`%s`' % k)
            values.append(self._encode(v))
        return keys, values

    def insert(self, attributes):
        keys, values = self._prepare_params(None, attributes)
        query = 'REPLACE DELAYED INTO `%s` (%s) VALUES (%s)' % (self.name,
                                                                ', '.join(keys),
                                                                ', '.join(values))
        self._execute(query)

    def update(self, id, attributes):
        keys, values = self._prepare_params(None, attributes)
        # Mutable
        args = ', '.join('%s=%s' % (k, v) for k, v in zip(keys, values))
        query = 'UPDATE `%s` SET %s WHERE `_id` = %s' % (self.name, args, self._encode(id))
        self._execute(query)

    def select(self, query_string):
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        self._execute(query_string, cursor=cursor)
        return iter(cursor)

    def drop(self):
        self._execute('DROP TABLE `%s`' % self.name)

    def _execute(self, query_string, cursor=None):
        print query_string
        if not cursor:
            cursor = self.connection.cursor()
        return cursor.execute(query_string)


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
        self._pending_updates[attr] = value

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
        # TODO: do something.
        # dict = self._raw_object_store.find_one({'_id' : self.id})
        # self._init_from_dict(dict)
        pass

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
                if hasattr(self, 'id'):
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
            self._pending_updates.setdefault('type', self.type)
        if hasattr(self, 'id'):
            self._pending_updates.setdefault('_id', self.id)
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

    @classmethod
    def get_all(cls, sha1):
        return cls._object_store.find({cls.key1_name : sha1})

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
    blob_id = make_persistent_attribute('blob_id')
    tree_id = make_persistent_attribute('tree_id')

    name = make_persistent_attribute('name')
    mode = make_persistent_attribute('mode')


class BlobTag(GitObjectAssociation):
    __tablename__ = 'blob_tags'
    _save_list = []
    _cache = {}
    key1_name = 'blob_id'
    key2_name = 'tag_id'
    blob_id = make_persistent_attribute('blob_id')
    tag_id = make_persistent_attribute('tag_id')



class TreeParentTree(GitObjectAssociation):
    __tablename__ = 'tree_parent_trees'
    _save_list = []
    _cache = {}
    key1_name = 'tree_id'
    key2_name = 'parent_tree_id'
    tree_id = make_persistent_attribute('tree_id')
    parent_tree_id = make_persistent_attribute('parent_tree_id')

    name = make_persistent_attribute('name')
    mode = make_persistent_attribute('mode')


class TreeCommit(GitObjectAssociation):
    __tablename__ = 'tree_commits'
    _save_list = []
    _cache = {}
    key1_name = 'tree_id'
    key2_name = 'commit_id'
    tree_id = make_persistent_attribute('tree_id')
    commit_id = make_persistent_attribute('commit_id')


class TreeTag(GitObjectAssociation):
    __tablename__ = 'tree_tags'
    _save_list = []
    _cache = {}
    key1_name = 'tree_id'
    key2_name = 'tag_id'
    tree_id = make_persistent_attribute('tree_id')
    tag_id = make_persistent_attribute('tag_id')


class CommitParentCommit(GitObjectAssociation):
    __tablename__ = 'commit_parent_commits'
    _save_list = []
    _cache = {}
    key1_name = 'commit_id'
    key2_name = 'parent_commit_id'
    commit_id = make_persistent_attribute('commit_id')
    parent_commit_id = make_persistent_attribute('parent_commit_id')



class CommitTree(GitObjectAssociation):
    __tablename__ = 'commit_trees'
    _save_list = []
    _cache = {}
    key1_name = 'commit_id'
    key2_name = 'tree_id'
    commit_id = make_persistent_attribute('commit_id')
    tree_id = make_persistent_attribute('tree_id')

    name = make_persistent_attribute('name')
    mode = make_persistent_attribute('mode')


class CommitTag(GitObjectAssociation):
    __tablename__ = 'commit_tags'
    _save_list = []
    _cache = {}
    key1_name = 'commit_id'
    key2_name = 'tag_id'
    commit_id = make_persistent_attribute('commit_id')
    tag_id = make_persistent_attribute('tag_id')


class TagParentTag(GitObjectAssociation):
    __tablename__ = 'tag_parent_tags'
    _save_list = []
    _cache = {}
    key1_name = 'tag_id'
    key2_name = 'parent_tag_id'
    commit_id = make_persistent_attribute('tag_id')
    parent_tag_id = make_persistent_attribute('parent_tag_id')

class GitObjectRepository(GitObjectAssociation):
    __tablename__ = 'git_object_repositories'
    _save_list = []
    _cache = {}
    key1_name = 'git_object_id'
    key2_name = 'repository_id'
    git_object_id = make_persistent_attribute('git_object_id')
    repository_id = make_persistent_attribute('repository_id')


class CommitParentCommit(GitObjectAssociation):
    __tablename__ = 'commit_parent_commits'
    _save_list = []
    _cache = {}
    key1_name = 'commit_id'
    key2_name = 'parent_commit_id'
    commit_id = make_persistent_attribute('commit_id')
    parent_commit_id = make_persistent_attribute('parent_commit_id')


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

    def limited_repositories(self, limit):
        return self.repositories.limit(limit)

    def add_tag(self, tag_id):
        raise AbstractMethodError()

    @property
    def tags(self):
        return Tag.find_matching(self.tag_ids)

    def add_repository(self, repository_id, recursive=False):
        repository_id = canonicalize_to_id(repository_id)
        gor = GitObjectRepository(self.id, repository_id)
        gor.save()


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

    def limited_parent_ids(self, limit):
        return self.parent_ids.limit(limit)

    @property
    def names(self):
        s = set(name for (id, name) in self.parent_ids_with_names)
        return Map(s, lambda x: x, count=len(s))

    def limited_names(self, limit):
        s = set(name for (id, name) in self.parent_ids_with_names.limit(limit))
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

    def limited_commit_ids(self, limit):
        return self.commit_ids.limit(limit)

    @property
    def commits(self):
        return Commit.find_matching(self.commit_ids)

    @property
    def parent_ids(self):
        return Map(self.parent_ids_with_names, lambda (id, name): id)

    def limited_parent_ids(self, limit):
        return self.parent_ids.limit(limit)

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
    # object_id = make_persistent_attribute('object_id')

    def add_tag(self, tag_id):
        tag_id = canonicalize_to_id(tag_id)
        b = TagParentTag(key1=self.id, key2=tag_id)
        b.save()

    def set_object_id(self, object_id):
        # object_id = canonicalize_to_id(object_id)
        # self.object_id = object_id
        pass

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
        for parent_id in parent_ids:
            cpc = CommitParentCommit(self.id, parent_id)
            cpc.save()

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
    last_index = make_persistent_attribute('last_index',
                                           default=datetime.datetime(1970,1,1),
                                           extractor=datetime_extractor)
    indexing = make_persistent_attribute('indexing',
                                         default=False,
                                         extractor=bool_extractor)
    been_indexed = make_persistent_attribute('been_indexed',
                                             default=False,
                                             extractor=bool_extractor)
    approved = make_persistent_attribute('approved',
                                         default=False,
                                         extractor=bool_extractor)
    count = make_persistent_attribute('count',
                                      default=0,
                                      extractor=int)

    _remote_heads = make_persistent_attribute('_remote_heads', default='')
    _new_remote_heads = make_persistent_attribute('_new_remote_heads', default='')

    def __init__(self, *args, **kwargs):
        super(Repository, self).__init__(*args, **kwargs)
        # Hack to default this, thus persisting it.
        self.indexing

    @property
    def clean_remote_heads(self):
        return Map(Commit.find_matching(self.remote_heads, dirty=False), lambda c: c.id)

    @property
    def remote_heads(self):
        return (self._remote_heads or '').split(',')

    @property
    def new_remote_heads(self):
        return (self._new_remote_heads or '').split(',')

    @classmethod
    def get_indexed_before(cls, date):
        """Get all repos indexed before the given date and not currently
        being indexed."""
        # Hack: should be lazier about this.
        if date is not None:
            return cls._object_store.find({'last_index' : {'$lt' : date},
                                           'indexing' : 0,
                                           'approved' : 1})
        else:
            return cls._object_store.find({'approved' : 1,
                                           'indexing' : 0})

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

    # TODO: use this
    # def set_new_remote_heads(self, new_remote_heads):
    #     current_remote_heads = set(self.remote_heads)
    #     new_remote_heads = set(new_remote_heads)
    #     to_add = new_remote_heads - current_remote_heads
    #     to_remove = current_remote_heads - new_remote_heads
    #     for remote_head in to_remove:
    #         rrh = RepositoryRemoteHead(repository_id=remote_head, remote_head_id=remote_head)
    #         rrh.delete()
    #     for remote_head in to_add:
    #         rrh = RepositoryRemoteHead(repository_id=remote_head, remote_head_id=remote_head)
    #         rrh.is_new = True
    #         rrh.save()

    def set_new_remote_heads(self, new_remote_heads):
        self._new_remote_heads = ','.join(new_remote_heads)

    def set_remote_heads(self, remote_heads):
        self._remote_heads = ','.join(remote_heads)

    def __str__(self):
        return 'Repository: %s' % self.url


class Aggregate(MongoDbModel, common.CommonMixin):
    """Singleton class that contains aggregate data about the indexer"""
    __tablename__ = 'aggregate'
    instance = None
    _save_list = []

    indexed_repository_count = make_persistent_attribute('indexed_repository_count',
                                                         default=0,
                                                         extractor=0)
    blob_count = make_persistent_attribute('blob_count', default=0, extractor=0)
    tree_count = make_persistent_attribute('tree_count', default=0, extractor=0)
    commit_count = make_persistent_attribute('commit_count', default=0, extractor=0)
    tag_count = make_persistent_attribute('tag_count', default=0, extractor=0)

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
