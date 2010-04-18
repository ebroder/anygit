import datetime
from dulwich import client, object_store, pack
import logging
import os
import tempfile

from anygit import models
from anygit.data import exceptions

try:
    import multiprocessing
except ImportError:
    import processing as multiprocessing

logger = logging.getLogger(__name__)

def fetch(repo):
    logger.info('Fetching from %s' % repo)
    def determine_wants(refs_dict):
        # Strictly speaking, this only needs to return strings.
        logger.debug('Called determine_wants on %s' % refs_dict)
        matching_commits = [c.id for c in models.Commit.find_matching(refs_dict.itervalues())
                            if c.complete]
        missing_commits = set(v for k, v in refs_dict.iteritems() if '^{}' not in k) - set(matching_commits)
        return list(missing_commits)

    def get_parents(sha1):
        try:
            c = models.Commit.get_by_attributes(id=sha1, complete=True)
        except exceptions.DoesNotExist:
            return []
        else:
            return [p.id for p in c.parents]

    destfd, destfile_name = tempfile.mkstemp()
    destfile = os.fdopen(destfd, 'w')
    logger.debug('Writing to %s' % destfile_name)
    def pack_data(data):
        destfile.write(data)

    def progress(progress):
        logger.debug('Received: %r' % progress)

    graph_walker = object_store.ObjectStoreGraphWalker(repo.remote_heads,
                                                       get_parents)
    c = client.TCPGitClient(repo.host)
    c.fetch_pack(path=repo.path,
                 determine_wants=determine_wants,
                 graph_walker=graph_walker,
                 pack_data=pack_data,
                 progress=progress)
    destfile.close()
    return destfile_name

def _get_objects_iterator(data, is_path):
    if is_path:
        pack_data = pack.PackData.from_path(data)
    else:
        file = StringIO.StringIO(data)
        length = len(data)
        pack_data = pack.PackData.from_file(file, length)
    uncompressed_pack = pack.Pack.from_objects(pack_data, None)
    return uncompressed_pack

def _process_data(repo, uncompressed_pack):
    logger.info('Starting object creation for %s' % repo)
    for obj in uncompressed_pack.iterobjects():
        object_type = obj._type
        logger.debug('About to create %s %s' % (object_type, obj.id))
        if object_type == 'blob':
            b = models.Blob.get_or_create(id=obj.id)
        elif object_type == 'tree':
            t = models.Tree.get_or_create(id=obj.id)
        elif object_type == 'commit':
            c = models.Commit.get_or_create(id=obj.id)
        elif object_type == 'tag':
            t = models.Tag.get_or_create(id=obj.id)
        else:
            raise ValueEror('Unrecognized type %s' % object_type)

    logger.info('Starting tree indexing for %s' % repo)
    trees = iter(o for o in uncompressed_pack.iterobjects() if o._type == 'tree')
    for tree in trees:
        t = models.Tree.get(id=tree.id)
        for _, _, sha1 in tree.iteritems():
            child = models.GitObject.get(sha1)
            child.add_commits(t.commits)
            child.save()
    del trees

    logger.info('Starting commit indexing for %s' % repo)
    commits = iter(o for o in uncompressed_pack.iterobjects() if o._type == 'commit')
    for commit in commits:
        c = models.Commit.get(id=commit.id)
        c.add_repository(repo)
        c.add_tree(commit.tree)
        c.add_parents(commit.parents)

    # TODO: might be able to del commits and go from there.
    logger.info('Marking commits complete for %s' % repo)
    commits = iter(o for o in uncompressed_pack.iterobjects() if o._type == 'commit')
    for commit in commits:
        c = models.Commit.get(id=commit.id)
        c.mark_complete()
        c.save()

    logger.info('Adding tags for %s' % repo)
    tags = iter(o for o in uncompressed_pack.iterobjects() if o._type == 'tag')
    for tag in tags:
        t = models.Tag.get_or_create(id=obj.id)
        t.set_object(obj.get_object())
        t.save()

def index_data(data, repo, is_path=False):
    if is_path:
        empty = not os.path.getsize(data)
    else:
        empty = data

    if empty:
        logger.error('No data to index')
        return
    objects_iterator = _get_objects_iterator(data, is_path)
    _process_data(repo, objects_iterator)

def fetch_and_index(repo):
    if isinstance(repo, str):
        repo = models.Repository.get(repo)
    repo.refresh()
    # There's a race condition here where two indexing processes might
    # try to index the same repo.  However, since it's idempotent,
    # this is not harmful beyond wasting resources.  However, we check
    # here to try to minimize the damage.
    if repo.indexing:
        logger.error('Repo is already being indexed')
        return
    logger.info('Beginning to index: %s' % repo)
    now = datetime.datetime.now()
    try:
        # Don't let other people try to index in parallel
        repo.indexing = True
        repo.save()
        models.flush()
        data_path = fetch(repo)
        index_data(data_path, repo, is_path=True)
        repo.last_update = now
    finally:
        repo.indexing = False
        repo.save()
        models.flush()
    logger.info('Done with %s' % repo)

def index_all(last_index=None, parallel=True):
    repos = models.Repository.get_indexed_before(last_index)
    logger.info('About to index the following repos: %s' % ', '.join([str(r) for r in repos]))
    if parallel:
        repo_ids = [r.id for r in repos]
        pool = multiprocessing.Pool(1, initializer=models.setup)
        pool.map(fetch_and_index, repo_ids)
    else:
        [fetch_and_index(repo) for repo in repos]
