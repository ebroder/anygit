import logging
from pylons import config
import pymongo
from pymongo import son_manipulator
import re

from anygit.backends import common
from anygit.data import exceptions


logger = logging.getLogger(__name__)

max_transaction_window = 10000
curr_transaction_window = 0

## Exported functions

def create_schema():
    # TODO: initialization work.
    pass

def init_model(connection):
    """Call me before using any of the tables or classes in the model."""
    git_objects_db = connection.anygit
    git_objects = git_objects_db.git_objects
    # Transform
    git_objects_db.add_son_manipulator(TransformGitObject())
    GitObject._object_store = git_objects

def setup():
    """
    Sets up the database session
    """
    connection = pymongo.Connection(config['mongodb.url'],
                                    config.get('mongodb.port', None))
    init_model(connection)

def flush():
    logger.debug('Committing...')
    classes = [GitObject]
    for klass in classes:
        logger.debug('Saving %d objects for %s' % (len(klass._save_list), klass))
        insert_list = set()
        update_list = set()
        for instance in klass._save_list:
            if instance.new:
                insert_list.add(instance)
            elif instance._pending_updates:
                update_list.add(instance)
            else:
                logger.info('Skipping unchanged object %s' % instance)
        klass._save_list.clear()
        
        if insert_list:
            klass._object_store.insert(insert_list)
            for instance in insert_list:
                instance.new = False
        for instance in update_list:
            klass._object_store.update({'_id' : instance.id}, instance._pending_updates)

        
## Internal functions

def classify(string):
    """Convert a class name to the corresponding class"""
    # mapping = {'Repository' : Repository,
    #            'Blob' : Blob,
    #            'Tree' : Tree,
    #            'Commit' : Commit}
    mapping = {'Blob' : Blob}
    try:
        return mapping[string]
    except KeyError:
        raise ValueError('No matching class found for %s' % string)

## Classes

class TransformGitObject(son_manipulator.SONManipulator):
    def transform_incoming(self, git_object, collection):
        """Transform a GitObject heading for the database"""
        return git_object.mongofy()

    def transform_outgoing(self, son, collection):
        """Transform a GitObject retrieved from the database"""
        klass = classify(son['__type__'])
        return klass.demongofy(son)

class MongoDbModel(object):
    # Should provide these in subclasses
    _object_store = None
    _save_list = None

    # Attributes: id, type

    def __init__(self, _raw_dict={}, **kwargs):
        kwargs.update(_raw_dict)
        self._init_from_dict(kwargs)
        self.new = True
        self._pending_updates = {}

    def _init_from_dict(self, dict):
        assert '_id' not in dict
        assert 'id' in dict
        for k, v in dict.iteritems():
            setattr(self, k, v)

    def _add_to_set(self, set_name, value):
        # TODO: to get the *right* semantics, should have a committed updates
        # and an uncommitted updates.
        adding = self._pending_updates.setdefault('$addToSet', {})
        target_set = adding.setdefault(set_name, {'$each' : set()})
        target_set['$each'].append(value)
    
    def _set(self, attr, value):
        setting = self._pending_updates.setdefault('$set', {})
        setting[attr] = value

    @property
    def type(self):
        return type(self).__name__

    @classmethod
    def get(cls, id):
        """Get an item with the given primary key"""
        return cls.get_by_attributes(id=id)

    @classmethod
    def get_by_attributes(cls, **kwargs):
        results = objects.find(kwargs)
        count = results.count()
        if count == 1:
            result = results.next()
            assert isinstance(result, cls)
            return result
        elif count == 0:
            raise exceptions.DoesNotExist('%s: %s' % (cls, kwargs))
        else:
            raise exceptions.NotUnique('%s: %s' % (cls, kwargs))

    @classmethod
    def all(cls):
        return self._object_store.find({'__type__' : cls.__name__})

    def refresh(self):
        raise NotImplementedError()

    def validate(self):
        """A stub method.  Should be overriden in subclasses."""
        pass

    def save(self):
        global curr_transaction_window
        self.validate()
        if not self._errors:
            self._save_list.add(self)
            if curr_transaction_window >= max_transaction_window:
                flush()
                curr_transaction_window = 0
            else:
                curr_transaction_window += 1
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

    def _init_from_dict(self, dict):
        super(GitObject, self)._init_from_dict(dict)
        if not hasattr(self, 'complete'):
            self.complete = False

    def mongofy(self, mongo_object):
        super(GitObject, self).mongofy(mongo_object)
        mongo_object['complete'] = self.complete
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
        return results.skip(offset).limit(limit)

    def mark_complete(self):
        self.complete = True
        self._set('complete', True)

class Blob(GitObject, common.CommonBlobMixin):
    """Represents a git Blob.  Has an id (the sha1 that identifies this
    object)"""
    # Attributes: parent_ids

    def _init_from_dict(self, dict):
        super(Blob, self)._init_from_dict(dict)
        if not hasattr(self, 'parent_ids'):
            self.parent_ids = []

    def mongofy(self, mongo_object=None):
        if mongo_object is None:
            mongo_object = {}
        super(Blob, self).mongofy(mongo_object)
        mongo_object['parent_ids'] = self.parent_ids
        return mongo_object

    def add_parent(self, parent):
        if isinstance(parent, GitObject):
            parent = parent.id
        elif not isinstance(parent, str):
            raise exceptions.ValidationError('Unknown parent type for %s' % parent)
        self.parent_ids.append(parent)
        self._add_to_set('parent', parent)

    @property
    def repositories(self):
        raise NotImplementedError()
        # return Session.query(Repository).join('commits', 'blobs').filter(Blob.id==self.id)

# class Tree(GitObject, common.CommonTreeMixin):
#     """
#     Represents a git Tree.  Has an id (the sha1 that identifies this
#     object)
#     """
#     __tablename__ = 'trees'
#     __mapper_args__ = {'polymorphic_identity': 'tree'}
#     id = sa.Column(sa.ForeignKey('git_objects.id'),
#                    primary_key=True)
#     subtrees = orm.relation('Tree',
#                             backref=orm.backref('parents',
#                                                 collection_class=set),
#                             primaryjoin=(id == trees_trees.c.parent_id),
#                             secondaryjoin=(id == trees_trees.c.subtree_id),
#                             secondary=trees_trees,
#                             collection_class=set)
#     blobs = orm.relation(Blob,
#                          backref=orm.backref('parents',
#                                              collection_class=set,
#                                              foreign_keys=[trees_blobs.c.tree_id,
#                                                            trees_blobs.c.blob_id]),
#                          primaryjoin=(id == trees_blobs.c.tree_id),
#                          secondaryjoin=(Blob.id == trees_blobs.c.blob_id),
#                          secondary=trees_blobs,
#                          foreign_keys=[trees_blobs.c.tree_id, trees_blobs.c.blob_id],
#                          collection_class=set)

#     def add_parent(self, parent):
#         self.parents.add(parent)

#     @property
#     def repositories(self):
#         return Session.query(Repository).join('commits', 'trees').filter(Tree.id==self.id)


# class Tag(GitObject, common.CommonTagMixin):
#     """
#     Represents a git Tree.  Has an id (the sha1 that identifies this
#     object)
#     """
#     __tablename__ = 'tags'
#     __mapper_args__ = {'polymorphic_identity': 'tag'}
#     id = sa.Column(sa.ForeignKey('git_objects.id'),
#                    primary_key=True)

#     # Should upgrade this someday to point to arbitrary objects.
#     commit_id = sa.Column(sa.ForeignKey('commits.id'))

#     def set_object(self, o):
#         o = canonicalize(o)
#         if o.type == 'commit':
#             self.commit = o
#         else:
#             logger.error('Could not set object %s as the target of %s' % (o, self))


# class Commit(GitObject, common.CommonCommitMixin):
#     """
#     Represents a git Commit.  Has an id (the sha1 that identifies
#     this object).  Also contains blobs, trees, and tags.
#     """
#     __tablename__ = 'commits'
#     __mapper_args__ = {'polymorphic_identity': 'commit'}
#     id = sa.Column(sa.ForeignKey('git_objects.id'),
#                    primary_key=True)

#     blobs = orm.relation(Blob,
#                          backref=orm.backref('commits',
#                                              collection_class=set),
#                          collection_class=set,
#                          secondary=blobs_commits)

#     trees = orm.relation(Tree,
#                          backref=orm.backref('commits',
#                                              collection_class=set),
#                          collection_class=set,
#                          secondary=trees_commits)

#     tags = orm.relation(Tag,
#                         backref='commit',
#                         collection_class=set,
#                         primaryjoin=(id == Tag.commit_id))

#     parents = orm.relation('Commit',
#                            backref=orm.backref('children',
#                                                collection_class=set),
#                            collection_class=set,
#                            primaryjoin=(id == commits_parents.c.child_id),
#                            secondaryjoin=(id == commits_parents.c.parent_id),
#                            secondary=commits_parents)

#     def add_repository(self, remote, recursive=False):
#         if isinstance(remote, str):
#             remote = Repository.get(remote)
#         if remote not in self.repositories:
#             self.repositories.add(remote)
#             if recursive:
#                 logger.debug('Recursively adding %s to %s' % (remote, self, recursive))
#                 for parent in self.parents:
#                     parent.add_repository(remote, recursive=True)

#     def add_tree(self, tree, recursive=True):
#         if isinstance(tree, str):
#             tree = Tree.get_or_create(id=tree)
#         # Assumes the invariant that if we have added a tree before,
#         # we have added all of its children as well
#         if tree not in self.trees:
#             self.trees.add(tree)
#             if recursive:
#                 for subtree in tree.subtrees:
#                     self.add_tree(subtree, recursive=True)
#                 for blob in tree.blobs:
#                     self.add_blob(blob)

#     def add_blob(self, blob):
#         if isinstance(blob, str):
#             blob = Blob.get_or_create(id=blob)
#         self.blobs.add(blob)

#     def add_parent(self, parent):
#         self.add_parents([parent])

#     def add_parents(self, parents):
#         logger.debug('Adding parents %s' % parents)
#         parents = set(canonicalize(p) for p in parents)
#         self.parents = self.parents.union(parents)

#     @classmethod
#     def find_matching(cls, sha1s):
#         """Given a list of sha1s, find the matching commit objects"""
#         return Session.query(cls).filter(cls.id.in_(sha1s))


# class RemoteHead(Base, SAMixin, common.CommonRemoteHeadMixin):
#     __tablename__ = 'remote_heads'
#     repo_id = sa.Column(sa.ForeignKey('repositories.id'),
#                         primary_key=True)
#     ref_id = sa.Column(sa.types.String(length=255), primary_key=True)
#     commit_id =  sa.Column(sa.ForeignKey('commits.id'))
#     commit = orm.relation(Commit,
#                           collection_class=set)

# class Repository(Base, SAMixin, common.CommonRepositoryMixin):
#     """
#     A git repository, corresponding to a remote in the-one-repo.git.
#     Contains many commits.
#     """
#     __tablename__ = 'repositories'
#     id = sa.Column(sa.types.String(length=40), primary_key=True)
#     url = sa.Column(sa.types.String(length=255), unique=True)
#     last_index = sa.Column(sa.types.DateTime())
#     indexing = sa.Column(sa.types.Boolean(), default=False)

#     commits = orm.relation(Commit,
#                            backref=orm.backref('repositories',
#                                                collection_class=set),
#                            collection_class=set,
#                            secondary=commits_repositories)

#     remote_heads = orm.relation(RemoteHead,
#                                 backref='repository',
#                                 collection_class=set)

#     @classmethod
#     def get_indexed_before(cls, date):
#         """Get all repos indexed before the given date and not currently
#         being indexed."""
#         if date is not None:
#             return Session.query(cls).filter(cls.last_index >= date).filter(cls.indexing == False).all()
#         else:
#             return Session.query(cls).filter(cls.indexing == False).all()

#     def __str__(self):
#         return 'Repository: %s' % self.url

