import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext import declarative


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


class GitObject(Base):
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


class Blob(GitObject):
    __tablename__ = 'blobs'
    __mapper_args__ = {'polymorphic_identity': 'blob'}
    name = sa.Column(sa.types.String(length=40),
                     sa.ForeignKey('git_objects.name'),
                     primary_key=True)


class Tree(GitObject):
    __tablename__ = 'trees'
    __mapper_args__ = {'polymorphic_identity': 'tree'}
    name = sa.Column(sa.types.String(length=40),
                     sa.ForeignKey('git_objects.name'),
                     primary_key=True)


class Tag(GitObject):
    __tablename__ = 'tags'
    __mapper_args__ = {'polymorphic_identity': 'tag'}
    name = sa.Column(sa.types.String(length=40),
                     sa.ForeignKey('git_objects.name'),
                     primary_key=True)

    commit = sa.Column(sa.types.String(length=40),
                       sa.ForeignKey('commits.name'))


class Commit(GitObject):
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
                        collection_class=set,
                        cascade='all')


class Repository(Base):
    __tablename__ = 'repositories'
    name = sa.Column(sa.types.String(length=40), primary_key=True)
    url = sa.Column(sa.types.String(length=255), unique=True)

    commits = orm.relation(Commit,
                           backref=orm.backref('repositories',
                                               collection_class=set),
                           collection_class=set,
                           secondary=commits_repositories)
