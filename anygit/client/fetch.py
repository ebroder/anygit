import datetime
from dulwich import client, object_store, pack
import logging
import os
import tempfile
import traceback

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
        logger.debug('Called determine_wants for %s' % repo)
        matching_commits = set(c.id for c in models.Commit.find_matching(refs_dict.itervalues())
                               if c.complete)
        remote_heads = set(v for k, v in refs_dict.iteritems() if '^{}' not in k)
        missing_commits = remote_heads - matching_commits
        # The commits we already have
        present_commits = remote_heads.intersection(matching_commits)
        for commit in models.Commit.find_matching(present_commits):
            commit.add_repository(repo, recursive=True)
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
        pass

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

    logger.info('Setting tree children for %s' % repo)
    trees = iter(o for o in uncompressed_pack.iterobjects() if o._type == 'tree')
    for tree in trees:
        t = models.Tree.get(id=tree.id)
        for _, _, sha1 in tree.iteritems():
            try:
                child = models.GitObject.get(sha1)
            except exceptions.DoesNotExist:
                logger.error('Could not find child %s of %s' % (sha1, t.id))
            else:
                if child.type == 'tree' or child.type == 'blob':
                    child.add_parent(t)
                    child.save()

    logger.info('Starting commit indexing for %s' % repo)
    commits = iter(o for o in uncompressed_pack.iterobjects() if o._type == 'commit')
    for commit in commits:
        c = models.Commit.get(id=commit.id)
        c.add_repository(repo, recursive=False)
        c.add_tree(commit.tree, recursive=True)
        c.add_parents(commit.parents)

    # TODO: might be able to del commits and go from there.
    logger.info('Marking objects complete for %s' % repo)
    objects = iter(o for o in uncompressed_pack.iterobjects())
    for object in objects:
        try:
            o = models.GitObject.get(id=object.id)
        except exceptions.DoesNotExist:
            logger.error('Could not find object %s!' % object.id)
        else:
            o.mark_complete()
            o.save()

    logger.info('Adding tags for %s' % repo)
    tags = iter(o for o in uncompressed_pack.iterobjects() if o._type == 'tag')
    for tag in tags:
        t = models.Tag.get_or_create(id=tag.id)
        t.set_object(tag.get_object())
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
        repo.last_index = now
        repo.save()
    except Exception, e:
        logger.error('Had a problem: %s' % traceback.format_exc())
    finally:
        repo.indexing = False
        repo.save()
        models.flush()
    logger.info('Done with %s' % repo)

def index_all(last_index=None, parallel=True):
    repos = models.Repository.get_indexed_before(last_index)
    logger.info('About to index %d repos' % len(repos))
    if parallel:
        repo_ids = [r.id for r in repos]
        pool = multiprocessing.Pool(5, initializer=models.setup)
        pool.map(fetch_and_index, repo_ids)
    else:
        [fetch_and_index(repo) for repo in repos]
