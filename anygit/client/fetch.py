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

def fetch(repo, recover_mode=False, discover_only=False, get_count=False):
    """Fetch data from a remote.  If recover_mode, will fetch all data
    as if we had indexed none of it.  Otherwise will do the right thing
    with the pack protocol.  If discover_only, will fetch no data."""
    logger.info('Fetching from %s' % repo)
    def determine_wants(refs_dict):
        # We don't want anything, just seeing if you exist.
        if discover_only:
            return []

        # If we already have some commits, it's possible they're from
        # a different repo.  We could recursively add them, but this
        # requires a lot of database reads, which is unfortunately a
        # luxury we don't have.  Thus we only report the remote heads
        # that we have already seen from this repo.
        if not recover_mode and not get_count:
            matching_commits = set(c.id for c in models.Commit.find_matching(refs_dict.itervalues(),
                                                                             dirty=False)
                                   if repo.id in c.repository_ids)
        else:
            matching_commits = set()
        remote_heads = set(v for k, v in refs_dict.iteritems() if '^{}' not in k)
        # Don't clobber the existing remote heads just yet, in case we crash
        repo.set_new_remote_heads(remote_heads)
        repo.save()
        missing_commits = remote_heads - matching_commits
        # The commits we already have
        present_commits = remote_heads.intersection(matching_commits)
        logger.debug('Requesting %d remote heads for %s.' % (len(missing_commits), repo))
        return list(missing_commits)

    def get_parents(sha1):
        try:
            c = models.Commit.get_by_attributes(id=sha1, dirty=False)
        except exceptions.DoesNotExist:
            return []
        else:
            return [p.id for p in c.parents if p.type == 'commit' and repo.id in p.repository_ids]

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

def _objectify(id, type):
    mapper = {'blob' : models.Blob,
              'tree' : models.Tree,
              'commit' : models.Commit,
              'tag' : models.Tag}
    return mapper[type].get_from_cache_or_new(id=id)

def _get_objects_iterator(data, is_path):
    if is_path:
        pack_data = pack.PackData.from_path(data)
    else:
        file = StringIO.StringIO(data)
        length = len(data)
        pack_data = pack.PackData.from_file(file, length)
    uncompressed_pack = pack.Pack.from_objects(pack_data, None)
    return uncompressed_pack

def _process_object(repo, obj, progress, type_mapper):
    # obj is Dulwich object
    # indexed_object will be the MongoDBModel we create
    progress(obj)

    if obj._type == 'tree':
        indexed_object = models.Tree.get_from_cache_or_new(id=obj.id)
        for name, mode, sha1 in obj.iteritems():
            # Default the type of the child object to a commit (a submodule)
            child_type = type_mapper.setdefault(sha1, 'commit')
            if child_type == 'tree':
                child = models.Tree.get_from_cache_or_new(id=sha1)
                child.add_parent(indexed_object, name=name, mode=mode)
            elif child_type == 'blob':
                child = models.Blob.get_from_cache_or_new(id=sha1)
                child.add_parent(indexed_object, name=name, mode=mode)
            else:
                assert child_type == 'commit'
                child = models.Commit.get_from_cache_or_new(id=sha1)
                child.add_as_submodule_of(indexed_object, name=name, mode=mode)
            child.save()
    elif obj._type == 'commit':
        indexed_object = models.Commit.get_from_cache_or_new(id=obj.id)
        indexed_object.add_parents(obj.parents)

        child = models.Tree.get_from_cache_or_new(id=obj.tree)
        child.add_commit(indexed_object)
        child.save()
    elif obj._type == 'tag':
        child, child_id = obj.get_object()
        child_type = child._type

        indexed_object = models.Tag.get_from_cache_or_new(id=obj.id)
        indexed_object.set_object_id(child_id)

        child = _objectify(id=child_id, type=child_type)
        child.add_tag(indexed_object)
        child.save()
    elif obj._type == 'blob':
        indexed_object = models.Blob.get_from_cache_or_new(id=obj.id)
    else:
        raise ValueError('Unrecognized git object type %s' % obj._type)
    indexed_object.save()

def _process_data(repo, uncompressed_pack, progress):
    logger.info('Dirtying objects for %s' % repo)
    type_mapper = {}
    for obj in uncompressed_pack.iterobjects():
        type_mapper[obj.id] = obj._type
        dirty = _objectify(id=obj.id, type=obj._type)
        dirty.mark_dirty(True)
        dirty.add_repository(repo)
        dirty.save()
    logger.info('Constructed object type map of size %s (%d bytes) for %s' %
                (len(type_mapper), type_mapper.__sizeof__(), repo))
    models.flush()

    logger.info('Now processing objects for %s' % repo)
    for obj in uncompressed_pack.iterobjects():
        _process_object(repo=repo,
                        obj=obj,
                        progress=progress,
                        type_mapper=type_mapper)
    del type_mapper

    logger.info('Cleaning objects for %s' % repo)
    type_mapper = {}
    for obj in uncompressed_pack.iterobjects():
        dirty = _objectify(id=obj.id, type=obj._type)
        dirty.mark_dirty(False)
        dirty.save()

def index_data(data, repo, is_path=False):
    if is_path:
        empty = not os.path.getsize(data)
    else:
        empty = data

    if empty:
        logger.info('No data to index')
        return
    objects_iterator = _get_objects_iterator(data, is_path)
    counter = {'count' : 0}
    def progress(object):
        counter['count'] += 1
        if not counter['count'] % 10000:
            check_for_die_file()
            logger.info('About to process object %d for %s (object is %s %s)' % (counter['count'],
                                                                                 repo,
                                                                                 object._type,
                                                                                 object.id))
    _process_data(repo, objects_iterator, progress)

def fetch_and_index(repo, recover_mode=False):
    check_for_die_file()
    if isinstance(repo, basestring):
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
    data_path = None
    try:
        # Don't let other people try to index in parallel
        repo.indexing = True
        repo.save()
        models.flush()
        data_path = fetch(repo, recover_mode=recover_mode)
        index_data(data_path, repo, is_path=True)
        repo.last_index = now
        repo.been_indexed = True
        # Finally, clobber the old remote heads.
        repo.set_remote_heads(repo.new_remote_heads)
        repo.set_new_remote_heads([])
        repo.save()
    except Exception, e:
        logger.error('Had a problem: %s' % traceback.format_exc())
    finally:
        if data_path:
            try:
                os.unlink(data_path)
            except IOError, e:
                logger.error('Could not remove tmpfile %s.: %s' % (data_path, e))
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
    except:
        logger.error(traceback.format_exc())
        raise

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

def refresh_all_counts():
    aggregator = models.Aggregate.get()
    aggregator.refresh_all_counts()
    aggregator.save()
    models.flush()
