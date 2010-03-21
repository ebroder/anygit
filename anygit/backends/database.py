from pylons import config
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext import declarative

from anygit.backends import common


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
    sa.Column('commit_name',
              sa.types.String(length=40),
              sa.ForeignKey('commits.name'),
              primary_key=True),
    sa.Column('repository_name',
              sa.types.String(length=40),
              sa.ForeignKey('repositories.name'),
              primary_key=True))

# Join table for blob -> commits mapping
blobs_commits = sa.Table(
    'blobs_commits',
    Base.metadata,
    sa.Column('blob_name',
              sa.types.String(length=40),
              sa.ForeignKey('blobs.name'),
              primary_key=True),
    sa.Column('commit_name',
              sa.types.String(length=40),
              sa.ForeignKey('commits.name'),
              primary_key=True))

# Join table for tree -> commits mapping
trees_commits = sa.Table(
    'trees_commits',
    Base.metadata,
    sa.Column('tree_name',
              sa.types.String(length=40),
              sa.ForeignKey('trees.name'),
              primary_key=True),
    sa.Column('commit_name',
              sa.types.String(length=40),
              sa.ForeignKey('commits.name'),
              primary_key=True))


class SAMixin(object):
    @classmethod
    def get(cls, id):
        """Retrieve an object by primary key"""
        return Session.query(cls).get(id)

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
    name = sa.Column(sa.types.String(length=40), primary_key=True)
    type = sa.Column(sa.types.String(length=50))

    __mapper_args__ = {'polymorphic_on': type}

    @classmethod
    def lookup_by_sha1(cls, sha1, partial=False):
        if partial:
            return Session.query(cls).filter(cls.name.startswith(sha1))
        else:
            return Session.query(cls).filter(cls.name == sha1)


class Blob(GitObject, common.CommonBlobMixin):
    """
    Represents a git Blob.  Has a name (the sha1 that identifies this
    object)
    """
    __tablename__ = 'blobs'
    __mapper_args__ = {'polymorphic_identity': 'blob'}
    name = sa.Column(sa.types.String(length=40),
                     sa.ForeignKey('git_objects.name'),
                     primary_key=True)


class Tree(GitObject, common.CommonTreeMixin):
    """
    Represents a git Tree.  Has a name (the sha1 that identifies this
    object)
    """
    __tablename__ = 'trees'
    __mapper_args__ = {'polymorphic_identity': 'tree'}
    name = sa.Column(sa.types.String(length=40),
                     sa.ForeignKey('git_objects.name'),
                     primary_key=True)


class Tag(GitObject, common.CommonTagMixin):
    """
    Represents a git Tree.  Has a name (the sha1 that identifies this
    object)
    """
    __tablename__ = 'tags'
    __mapper_args__ = {'polymorphic_identity': 'tag'}
    name = sa.Column(sa.types.String(length=40),
                     sa.ForeignKey('git_objects.name'),
                     primary_key=True)

    commit_name = sa.Column(sa.types.String(length=40),
                            sa.ForeignKey('commits.name'))


class Commit(GitObject, common.CommonCommitMixin):
    """
    Represents a git Commit.  Has a name (the sha1 that identifies
    this object).  Also contains blobs, trees, and tags.
    """
    __tablename__ = 'commits'
    __mapper_args__ = {'polymorphic_identity': 'commit'}
    name = sa.Column(sa.types.String(length=40),
                     sa.ForeignKey('git_objects.name'),
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
                        primaryjoin=(name == Tag.commit_name))


class Repository(Base, SAMixin, common.CommonRepositoryMixin):
    """
    A git repository, corresponding to a remote in the-one-repo.git.
    Contains many commits.
    """
    __tablename__ = 'repositories'
    name = sa.Column(sa.types.String(length=40), primary_key=True)
    url = sa.Column(sa.types.String(length=255), unique=True)

    commits = orm.relation(Commit,
                           backref=orm.backref('repositories',
                                               collection_class=set),
                           collection_class=set,
                           secondary=commits_repositories)
