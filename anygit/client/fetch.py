import datetime
from dulwich import client, object_store, pack
import logging
import os
import sys
import tempfile
import threading
import traceback

from anygit import models
from anygit.data import exceptions

try:
    import multiprocessing
except ImportError:
    import processing as multiprocessing

DIR = os.path.dirname(__file__)
logger = logging.getLogger(__name__)
timeout = 10

class Error(Exception):
    pass

class DieFile(Error):
    pass

class Checker(threading.Thread):
    valid = None
    def __init__(self, repo):
        super(Checker, self).__init__()
        self.repo = repo

    def run(self):
        try:
            fetch(self.repo, discover_only=True)
        except Exception, e:
            logger.debug(traceback.format_exc(e))
            self.valid = False
        else:
            self.valid = True

def check_validity(repo):
    c = Checker(repo)
    c.start()
    c.join(timeout)
    # TODO: could still be open...
    if c.isAlive() or not c.valid:
        return False
    else:
        return True

def fetch(repo, recover_mode=False, discover_only=False):
    """Fetch data from a remote.  If recover_mode, will fetch all data
    as if we had indexed none of it.  Otherwise will do the right thing
    with the pack protocol.  If discover_only, will fetch no data."""
    logger.info('Fetching from %s' % repo)
    def determine_wants(refs_dict):
        # We don't want anything, just seeing if you exist.
        if discover_only:
            return []

        if not recover_mode:
            # Strictly speaking, this only needs to return strings.
            matching_commits = set(c.id for c in models.Commit.find_matching(refs_dict.itervalues())
                                   if c.complete)
        else:
            matching_commits = set()
        remote_heads = set(v for k, v in refs_dict.iteritems() if '^{}' not in k)
        missing_commits = remote_heads - matching_commits
        # The commits we already have
        present_commits = remote_heads.intersection(matching_commits)
        # If we already have some commits, it's possible they're from
        # a different repo, so we should make sure that this one gets
        # them.  Note that these commits may actually be tags:
        for commit in models.GitObject.find_matching(present_commits):
            commit.add_repository(repo, recursive=True)
            commit.save()
        logger.debug('Requesting %d remote heads for %s.' % (len(missing_commits), repo))
        return list(missing_commits)

    def get_parents(sha1):
        try:
            c = models.Commit.get_by_attributes(id=sha1, complete=True)
        except exceptions.DoesNotExist:
            return []
        else:
            return [p.id for p in c.parents if p.type == 'commit']

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

count = 0
def _process_object(repo, object):
    global count
    count += 1
    if not count % 10000:
        check_for_die_file()
        logger.info('About to process object %d for %s (object is %s)' % (count,
                                                                          repo,
                                                                          object))
    if object._type == 'tree':
        try:
            t = models.Tree.get(id=object.id)
        except exceptions.DoesNotExist:
            logger.error('Apparently %s does not exist in %s...' % (object.id, repo))
        else:
            for name, mode, sha1 in object.iteritems():
                try:
                    child = models.GitObject.get(sha1)
                except exceptions.DoesNotExist:
                    logger.info("Could not find child %s of %s in repo %s, assuming "
                                "it's a commit from a submodule" % (sha1, t.id, repo))
                    child = models.Commit.get_or_create(id=sha1)
                if child.type in ['tree', 'blob']:
                    child.add_parent(t, name=name)
                else:
                    assert child.type == 'commit'
                    child.add_as_submodule_of(t, name=name)
                child.save()
                t.add_child(child)
            t.save()
    elif object._type == 'commit':
        try:
            c = models.Commit.get(id=object.id)
            c.set_tree(object.tree)
            c.add_parents(object.parents)
            c.save()

            child = models.Tree.get_or_create(id=object.tree)
            child.add_commit(c)
            child.save()
        except exceptions.DoesNotExist, e:
            logger.critical('Had trouble with %s, error:\n%s!' % (object, traceback.format_exc(e)))
    elif object._type == 'tag':
        t = models.Tag.get_or_create(id=object.id)
        t.add_repository(repo, recursive=False)
        t.save()


def _process_data(repo, uncompressed_pack):
    logger.info('Starting object creation for %s' % repo)
    for obj in uncompressed_pack.iterobjects():
        if obj._type == 'blob':
            o = models.Blob.get_or_create(id=obj.id)
        elif obj._type == 'tree':
            o = models.Tree.get_or_create(id=obj.id)
        elif obj._type == 'commit':
            o = models.Commit.get_or_create(id=obj.id)
        elif obj._type == 'tag':
            o = models.Tag.get_or_create(id=obj.id)
        else:
            raise ValueEror('Unrecognized type %s' % object_type)
        o.add_repository(repo)
        o.save()
    models.flush()
    check_for_die_file()

    logger.info('Now processing objects for %s' % repo)
    for object in uncompressed_pack.iterobjects():
        _process_object(repo=repo, object=object)
    check_for_die_file()

    logger.info('Marking objects complete for %s' % repo)
    for object in uncompressed_pack.iterobjects():
        try:
            o = models.GitObject.get(id=object.id)
        except exceptions.DoesNotExist:
            logger.critical('Could not find object %s!' % object.id)
        else:
            o.mark_complete()
            o.save()
    check_for_die_file()

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

def fetch_and_index(repo, recover_mode=False):
    check_for_die_file()
    if isinstance(repo, str) or isinstance(repo, unicode):
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
        data_path = fetch(repo, recover_mode=recover_mode)
        index_data(data_path, repo, is_path=True)
        repo.last_index = now
        repo.been_indexed = True
        repo.save()
    except Exception, e:
        logger.error('Had a problem: %s' % traceback.format_exc())
    finally:
        repo.indexing = False
        repo.save()
        models.flush()
    logger.info('Done with %s' % repo)

def fetch_and_index_threaded(repo):
    models.setup()
    try:
        return fetch_and_index(repo)
    except DieFile:
        # TODO: do something to terminate the controller process too
        sys.exit(1)

def index_all(last_index=None, threads=1):
    repos = list(models.Repository.get_indexed_before(last_index))
    logger.info('About to index %d repos' % len(repos))
    if threads > 1:
        repo_ids = [r.id for r in repos]
        pool = multiprocessing.Pool(threads)
        pool.map(fetch_and_index_threaded, repo_ids)
    else:
        [fetch_and_index(repo) for repo in repos]

def check_for_die_file():
    if os.path.exists(os.path.join(DIR, 'die')):
        logger.info('Die file encountered; exiting')
        raise DieFile('Die file encountered')
