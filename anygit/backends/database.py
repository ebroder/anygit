from pylons import config
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext import declarative

from anygit.backends import common
from anygit.data import exceptions


Session = None
Engine = None
Base = declarative.declarative_base()
Metadata = Base.metadata


def init_model(engine):
    """Call me before using any of the tables or classes in the model."""
    global Session, Engine

    Engine = engine

    sm = orm.sessionmaker(autoflush=True, autocommit=False, bind=engine)
    Session = orm.scoped_session(sm)


def setup():
    """
    Sets up the database session
    """
    engine = sa.engine_from_config(config, 'sqlalchemy.')
    init_model(engine)

# Join table for commit -> repositories mapping
commits_repositories = sa.Table(
    'commits_repositories',
    Base.metadata,
    sa.Column('commit_id',
              sa.ForeignKey('commits.id'),
              primary_key=True),
    sa.Column('repository_id',
              sa.ForeignKey('repositories.id'),
              primary_key=True))

# Join table for blob -> commits mapping
blobs_commits = sa.Table(
    'blobs_commits',
    Base.metadata,
    sa.Column('blob_id',
              sa.ForeignKey('blobs.id'),
              primary_key=True),
    sa.Column('commit_id',
              sa.ForeignKey('commits.id'),
              primary_key=True))

# Join table for tree -> commits mapping
trees_commits = sa.Table(
    'trees_commits',
    Base.metadata,
    sa.Column('tree_id',
              sa.ForeignKey('trees.id'),
              primary_key=True),
    sa.Column('commit_id',
              sa.ForeignKey('commits.id'),
              primary_key=True))


class SAMixin(object):
    @classmethod
    def get(cls, id):
        """Retrieve an object by primary key"""
        instance = Session.query(cls).get(id)
        if instance:
            return instance
        else:
            raise exceptions.DoesNotExist('%s: %s' % (cls, id))

    @classmethod
    def get_by_attributes(cls, **kwargs):
        results = Session.query(cls)
        for key, value in kwargs.iteritems():
            results = results.filter(getattr(cls, key) == value)
        count = results.count()
        if count == 1:
            return list(results)[0]
        elif count == 0:
            raise exceptions.DoesNotExist('%s: %s' % (cls, kwargs))
        else:
            raise exceptions.NotUnique('%s: %s' % (cls, kwargs))

    @classmethod
    def all(cls):
        return Session.query(cls).all()

    def save(self):
        self.validate()
        if not self._errors:
            Session.add(self)
            Session.commit()
            return True
        else:
            return False


class GitObject(Base, SAMixin):
    """
    The base class for git objects (such as blobs, commits, etc..).
    Subclasses inherit via join table inheritance.
    """
    __tablename__ = 'git_objects'
    id = sa.Column(sa.types.String(length=40), primary_key=True)
    type = sa.Column(sa.types.String(length=50))

    __mapper_args__ = {'polymorphic_on': type}

    @classmethod
    def lookup_by_sha1(cls, sha1, partial=False):
        if partial:
            return Session.query(cls).filter(cls.id.startswith(sha1))
        else:
            return Session.query(cls).filter(cls.id == sha1)


class Blob(GitObject, common.CommonBlobMixin):
    """
    Represents a git Blob.  Has an id (the sha1 that identifies this
    object)
    """
    __tablename__ = 'blobs'
    __mapper_args__ = {'polymorphic_identity': 'blob'}
    id = sa.Column(sa.ForeignKey('git_objects.id'),
                   primary_key=True)

    def add_commit(self, commit):
        if isinstance(commit, str):
            commit = Commit.get(commit)
        self.commits.add(commit)
        self.save()

    @property
    def repositories(self):
        return Session.query(Repository).join('commits', 'blobs').filter(Blob.id==self.id)


class Tree(GitObject, common.CommonTreeMixin):
    """
    Represents a git Tree.  Has an id (the sha1 that identifies this
    object)
    """
    __tablename__ = 'trees'
    __mapper_args__ = {'polymorphic_identity': 'tree'}
    id = sa.Column(sa.ForeignKey('git_objects.id'),
                   primary_key=True)

    def add_commit(self, commit):
        if isinstance(commit, str):
            commit = Commit.get(commit)
        self.commits.add(commit)
        self.save()

    @property
    def repositories(self):
        return Session.query(Repository).join('commits', 'trees').filter(Tree.id==self.id)


class Tag(GitObject, common.CommonTagMixin):
    """
    Represents a git Tree.  Has an id (the sha1 that identifies this
    object)
    """
    __tablename__ = 'tags'
    __mapper_args__ = {'polymorphic_identity': 'tag'}
    id = sa.Column(sa.ForeignKey('git_objects.id'),
                   primary_key=True)

    commit_id = sa.Column(sa.ForeignKey('commits.id'))


class Commit(GitObject, common.CommonCommitMixin):
    """
    Represents a git Commit.  Has an id (the sha1 that identifies
    this object).  Also contains blobs, trees, and tags.
    """
    __tablename__ = 'commits'
    __mapper_args__ = {'polymorphic_identity': 'commit'}
    id = sa.Column(sa.ForeignKey('git_objects.id'),
                   primary_key=True)

    blobs = orm.relation(Blob,
                         backref=orm.backref('commits',
                                             collection_class=set),
                         collection_class=set,
                         secondary=blobs_commits)

    trees = orm.relation(Tree,
                         backref=orm.backref('commits',
                                             collection_class=set),
                         collection_class=set,
                         secondary=trees_commits)

    tags = orm.relation(Tag,
                        backref='commit',
                        collection_class=set,
                        primaryjoin=(id == Tag.commit_id))

    def add_repository(self, remote):
        if isinstance(remote, str):
            remote = Repository.get(remote)
        self.repositories.add(remote)

    def add_tree(self, tree):
        if isinstance(tree, str):
            tree = Tree.get_or_create(id=tree)
        self.trees.add(tree)

    def add_parent(self, parent):
        if isinstance(parent, str):
            parent = Commit.get_or_create(id=parent)
        self.parents.add(parent)

    @classmethod
    def find_matching(cls, sha1s):
        """Given a list of sha1s, find the matching commit objects"""
        return Session.query(cls).filter(cls.id.in_(sha1s))


class RemoteHead(Base, SAMixin, common.CommonRemoteHeadMixin):
    __tablename__ = 'remote_heads'
    repo_id = sa.Column(sa.ForeignKey('repositories.id'),
                        primary_key=True)
    ref_id = sa.Column(sa.types.String(length=255), primary_key=True)
    commit_id =  sa.Column(sa.ForeignKey('commits.id'))
    commit = orm.relation(Commit,
                          collection_class=set)

class Repository(Base, SAMixin, common.CommonRepositoryMixin):
    """
    A git repository, corresponding to a remote in the-one-repo.git.
    Contains many commits.
    """
    __tablename__ = 'repositories'
    id = sa.Column(sa.types.String(length=40), primary_key=True)
    url = sa.Column(sa.types.String(length=255), unique=True)

    commits = orm.relation(Commit,
                           backref=orm.backref('repositories',
                                               collection_class=set),
                           collection_class=set,
                           secondary=commits_repositories)

    remote_heads = orm.relation(RemoteHead,
                                backref='repository',
                                collection_class=set)
