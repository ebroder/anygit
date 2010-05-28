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

connection = None
collection_to_class = {}

sha1_re = re.compile('^[a-f0-9]*')

## Exported functions

def create_schema():
    print 'Huhh??'

def flush():
    pass

def init_model(connection):
    """Call me before using any of the tables or classes in the model."""
    db = connection

    for obj in globals().itervalues():
        if type(obj) == type and issubclass(obj, MysqlModel) and hasattr(obj, '__tablename__'):
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
                                 db=config.get('mysql.db'),
                                 ssl={'ca' : config.get('mysql.cert')})
    init_model(connection)

def destroy_session():
    global connection
    connection = None

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
    if isinstance(db_object, MysqlModel):
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

def datetime_extractor(d):
    if isinstance(d, basestring):
        return datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
    else:
        return d

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

    def skip(self, skip):
        return Map(self.result.skip(skip), self.fun, self._count)


class Query(object):
    def __init__(self, domain, query, is_full_query=None):
        self.is_full_query = is_full_query
        self._limit = None
        self._skip = None
        self._order = None
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
        self._iterator = None

    def _get_iterator(self):
        if not self._iterator:
            self._iterator = iter(self.domain.select(self._get_select()))
        return self._iterator

    def _get_order(self):
        if self._order:
            return ' ORDER BY `%s` %s' % self._order
        else:
            return ''

    def _get_select(self):
        # TODO: select a subset of attributes
        if self.is_full_query:
            return self.query

        if self.query:
            full_query = 'select * from `%s` where %s' % (self.domain.name, self.query)
        else:
            full_query = 'select * from `%s`' % self.domain.name
        return full_query + self._get_order() + self._get_limit()

    def _get_count(self):
        if self.query:
            full_query = 'select count(*) as count from `%s` where %s' % (self.domain.name, self.query)
        else:
            full_query = 'select count(*) from `%s`' % self.domain.name
        return full_query

    def _get_limit(self):
        clause = []
        if self._limit is not None:
            clause.append('LIMIT %d' % self._limit)
        if self._skip is not None:
            clause.append('OFFSET %d' % self._skip)
        if clause:
            return ' %s' % ' '.join(clause)
        else:
            return ''

    def __iter__(self):
        return iter(self.transform_outgoing(i) for i in self._get_iterator())

    def count(self):
        return int(self.domain.select(self._get_count()).next()['count'])

    def next(self):
        return self.transform_outgoing(self._get_iterator().next())

    def limit(self, limit):
        self._limit = limit
        return self

    def skip(self, skip):
        self._skip = skip
        return self

    def order(self, column, type):
        """Order the results.  type should be ASC or DESC"""
        self._order = (column, type)
        return self

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

    def find(self, kwargs='', is_full_query=None):
        return Query(self, kwargs, is_full_query=is_full_query)

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
            return repr(unicode(value)).lstrip('u')

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

    def insert(self, attributes, delayed=True):
        keys, values = self._prepare_params(None, attributes)
        if delayed:
            delayed_statement = ' DELAYED'
        else:
            delayed_statement = ''
        query = 'INSERT%s IGNORE INTO `%s` (%s) VALUES (%s)' % (delayed_statement,
                                                                self.name,
                                                                ', '.join(keys),
                                                                ', '.join(values))
        self._execute(query)

    def update(self, id, attributes):
        keys, values = self._prepare_params(None, attributes)
        # Mutable
        args = ', '.join('%s=%s' % (k, v) for k, v in zip(keys, values))
        query = 'UPDATE `%s` SET %s WHERE `id` = %s' % (self.name, args, self._encode(id))
        self._execute(query)

    def select(self, query_string):
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        self._execute(query_string, cursor=cursor)
        return iter(cursor)

    def drop(self):
        self._execute('DROP TABLE `%s`' % self.name)

    def _execute(self, query_string, cursor=None):
        if not cursor:
            cursor = self.connection.cursor()
        return cursor.execute(query_string)


class MysqlModel(object):
    # Should provide these in subclasses
    cache = {}
    mutable = False
    has_type = False

    def __init__(self, _raw_dict={}, **kwargs):
        self._pending_updates = {}
        self._init_from_dict(_raw_dict)
        self._pending_updates.clear()
        self._init_from_dict(kwargs)
        self.new = True
        self._pending_save = False
        self._changed = False

    def _init_from_dict(self, dict):
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
        return cls.get_by_attributes(id=id)

    @classmethod
    def get_from_cache_or_new(cls, id):
        return cls(id=id)

    @classmethod
    def get_from_cache(cls, id):
        return None

    @classmethod
    def get_by_attributes(cls, **kwargs):
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
        return cls._object_store.find(kwargs).count() > 0

    def refresh(self):
        # TODO: do something.
        # dict = self._raw_object_store.find_one({'id' : self.id})
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
            else:
                try:
                    updates = self.get_updates()
                    if not self.new:
                        self._object_store.update(self.id,
                                                  updates)
                    elif self.mutable:
                        self._object_store.insert(updates, delayed=False)
                        self._object_store.update(self.id,
                                                  updates)
                    else:
                        self._object_store.insert(updates)
                except:
                    logger.critical('Had some trouble saving %s' % self)
                    raise
                self.mark_saved()
            return True
        else:
            return False

    def delete(self):
        raise NotImplementedError()

    @classmethod
    def demongofy(cls, son):
        instance = cls(_raw_dict=son)
        instance.new = False
        return instance

    @classmethod
    def find_matching(cls, ids, **kwargs):
        """Given a list of ids, find the matching objects"""
        kwargs.update({'id' : { '$in' : list(ids) }})
        return cls._object_store.find(kwargs)

    def get_updates(self):
        # Hack to add *something* for new insertions
        if self.has_type:
            self._pending_updates.setdefault('type', self.type)
        if hasattr(self, 'id'):
            self._pending_updates.setdefault('id', self.id)
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


class GitObjectAssociation(MysqlModel, common.CommonMixin):
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
    def get_all(cls, sha1, key=None):
        if key is None:
            key = cls.key1_name
        return cls._object_store.find({key : sha1})

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


class GitObject(MysqlModel, common.CommonGitObjectMixin):
    """The base class for git objects (such as blobs, commits, etc..)."""
    __tablename__ = 'git_objects'
    _save_list = []
    _cache = {}

    @classmethod
    def lookup_by_sha1(cls, sha1, partial=False, skip=None, limit=10):
        if partial:
            results = cls._object_store.find_prefix('id', sha1)
        else:
            results = cls._object_store.find({'id' : sha1})
        count = results.count()
        return results.skip(skip).limit(limit), count

    @classmethod
    def all(cls):
        if cls == GitObject:
            return cls._object_store.find()
        else:
            return cls._object_store.find({'type' : cls.__name__.lower()})

    @property
    def repository_ids(self):
        return Map(GitObjectRepository.get_all(self.id), lambda gor: gor.repository_id)

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

    def add_repository(self, repository_id):
        repository_id = canonicalize_to_id(repository_id)
        gor = GitObjectRepository(self.id, repository_id)
        gor.save()

    @property
    def dirty(self):
        return bool_extractor(self._object_store.find('select count(*) from git_object_repositories as gor LEFT JOIN '
                                                      ' repositories as r on gor.repository_id = r.id where dirty = 1'))


class Blob(GitObject, common.CommonBlobMixin):
    """Represents a git Blob.  Has an id (the sha1 that identifies this
    object)"""
    has_type = True

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
    has_type = True

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
        return Map(TreeParentTree.get_all(self.id), lambda tpt: (tpt.parent_tree_id, tpt.name))

    def limited_names(self, limit):
        s = set(name for (id, name) in self.parent_ids_with_names.limit(limit))
        return Map(s, lambda x: x, count=len(s))

    @property
    def commit_ids(self):
        return Map(TreeCommit.get_all(self.id), lambda tc: tc.commit_id)

    def limited_commit_ids(self, limit):
        return self.commit_ids.limit(limit)

    @property
    def commits(self):
        return Commit.find_matching(self.commit_ids)

    def limited_parent_ids_with_names(self, limit):
        return self.parent_ids_with_names.limit(limit)

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
    has_type = True
    # object_id = make_persistent_attribute('object_id')

    def add_tag(self, tag_id):
        tag_id = canonicalize_to_id(tag_id)
        b = TagParentTag(key1=self.id, key2=tag_id)
        b.save()

    @property
    def object(self):
        return GitObject.get(id=self.object_id)


class Commit(GitObject, common.CommonCommitMixin):
    """Represents a git Commit.  Has an id (the sha1 that identifies
    this object).  Also contains blobs, trees, and tags."""
    has_type = True

    @property
    def child_ids(self):
        return Map(CommitParentCommit.get_all(self.id, key='parent_commit_id'), lambda cpc: cpc.commit_id)

    def limited_child_ids(self, limit):
        return self.child_ids.limit(limit)

    @property
    def parent_ids(self):
        return Map(CommitParentCommit.get_all(self.id), lambda cpc: cpc.parent_commit_id)

    def add_parent(self, parent):
        self.add_parents([parent])

    def add_parents(self, parent_ids):
        parent_ids = set(canonicalize_to_id(p) for p in parent_ids)
        for parent_id in parent_ids:
            cpc = CommitParentCommit(self.id, parent_id)
            cpc.save()

    def add_tree(self, tree_id):
        tree_id = canonicalize_to_id(tree_id)
        t = TreeCommit(key1=tree_id, key2=self.id)
        t.save()

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


class Repository(MysqlModel, common.CommonRepositoryMixin):
    """A git repository.  Contains many commits."""
    mutable = True
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
    dirty = make_persistent_attribute('dirty',
                                      default=False,
                                      extractor=bool_extractor)

    _remote_heads = make_persistent_attribute('_remote_heads', default='')
    _new_remote_heads = make_persistent_attribute('_new_remote_heads', default='')

    def __init__(self, *args, **kwargs):
        super(Repository, self).__init__(*args, **kwargs)
        # Hack to default this, thus persisting it.
        self.indexing

    @property
    def clean_remote_heads(self):
        if self.dirty:
            return Map([], lambda x: x, count=0)
        else:
            return Map(Commit.find_matching(self.remote_heads), lambda c: c.id)

    @property
    def remote_heads(self):
        return (self._remote_heads or '').split(',')

    @property
    def new_remote_heads(self):
        return (self._new_remote_heads or '').split(',')

    @classmethod
    def get_indexed_before(cls, date, approved=None):
        """Get all repos indexed before the given date and not currently
        being indexed."""
        if approved is None:
            approved = 1

        # Hack: should be lazier about this.
        if date is not None:
            return cls._object_store.find({'last_index' : {'$lt' : date},
                                           'indexing' : 0,
                                           'approved' : approved})
        else:
            return cls._object_store.find({'approved' : approved,
                                           'indexing' : 0})

    @classmethod
    def get_by_highest_count(cls, n=None, descending=True):
        if descending:
            order = 'DESC'
        else:
            order = 'ASC'

        if n:
            limit = ' LIMIT %d' % n
        else:
            limit = ''

        #query = ('select * from (select repository_id from git_object_repositories group by '
        #        'repository_id order by count(*) %s%s) as assoc LEFT JOIN repositories '
        #         'on assoc.repository_id = repositories.id') % (order, limit)
        return cls._object_store.find().order('count', order).limit(n)

    def set_count(self, value):
        self.count = value

    def count_objects(self):
        return GitObjectRepository._object_store.find({'repository_id' : self.id}).count()

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


class Aggregate(MysqlModel, common.CommonMixin):
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
            pass
        
        def __exit__(self, type, value, traceback):
            pass

    @classmethod
    def get(cls):
        if not cls.instance:
            try:
                cls.instance = super(Aggregate, cls).get(id='main')
            except exceptions.DoesNotExist:
                cls.instance = cls.create(id='main')
                flush()
        return cls.instance

    def refresh_all_counts(self, all=None):
        if all:
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

