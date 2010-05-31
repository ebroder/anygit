import datetime
from dulwich import client, object_store
import logging
import os
import sys
import tempfile
import threading
import traceback

from anygit import models
from anygit.client import git_parser
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


class DeadRepo(Error):
    pass


class Checker(threading.Thread):
    valid = None
    def __init__(self, repo):
        super(Checker, self).__init__()
        self.repo = repo

    def run(self):
        if not self.repo.url:
            return

        try:
            fetch(self.repo, state={}, discover_only=True)
        except Exception, e:
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

def fetch(repo, state, recover_mode=False, discover_only=False,
          get_count=False, packfile=None, batch=None, unpack=False):
    """Fetch data from a remote.  If recover_mode, will fetch all data
    as if we had indexed none of it.  Otherwise will do the right thing
    with the pack protocol.  If discover_only, will fetch no data."""
    if packfile:
        return packfile

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
        if not recover_mode and not get_count and not repo.dirty:
            matching_commits = set(c.id for c in models.Commit.find_matching(refs_dict.itervalues())
                                   if repo.id in c.repository_ids)
        else:
            matching_commits = set()
        remote_heads = set(v for k, v in refs_dict.iteritems() if '^{}' not in k)
        remote_heads -= state.setdefault('retrieved', set())
        # Don't clobber the existing remote heads just yet, in case we crash
        repo.set_new_remote_heads(remote_heads)
        repo.save()
        missing_commits = remote_heads - matching_commits
        # The commits we already have
        present_commits = remote_heads.intersection(matching_commits)
        logger.debug('Requesting %d remote heads for %s.' % (len(missing_commits), repo))
        wants = list(missing_commits)
        if batch:
            logger.info("I'd really like %d commits, but only requesting %d due to batch size" %
                        (len(wants), batch))
            if len(wants) > batch:
                state['has_extra'] = True
            else:
                state['has_extra'] = False
            wants = wants[0:batch]
        l = state.setdefault('retrieved', set())
        l.update(wants)
        return wants

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

    graph_walker = object_store.ObjectStoreGraphWalker(repo.clean_remote_heads,
                                                       get_parents)
    assert repo.host
    assert repo.path
    c = client.TCPGitClient(repo.host)
    try:
        c.fetch_pack(path=repo.path,
                     determine_wants=determine_wants,
                     graph_walker=graph_walker,
                     pack_data=pack_data,
                     progress=progress)
    except KeyboardInterrupt:
        pass
    except Exception, e:
        logger.error('Problem when fetching %s: %s' % (repo, traceback.format_exc()))
        raise DeadRepo
    destfile.close()
    return destfile_name

def _objectify(id, type):
    mapper = {'blob' : models.Blob,
              'tree' : models.Tree,
              'commit' : models.Commit,
              'tag' : models.Tag}
    return mapper[type].get_from_cache_or_new(id=id)

def _process_object(repo, obj, progress, type_mapper, iteration):
    # obj is Dulwich object
    # indexed_object will be the MongoDBModel we create
    progress(obj)

    if iteration == 1:
        if obj.type_name == 'tree':
            indexed_object = models.Tree.get_from_cache_or_new(id=obj.id)
        elif obj.type_name == 'commit':
            indexed_object = models.Commit.get_from_cache_or_new(id=obj.id)
            indexed_object.add_parents(obj.parents)
            indexed_object.add_tree(obj.tree)
        elif obj.type_name == 'tag':
            indexed_object = models.Tag.get_from_cache_or_new(id=obj.id)
        else:
            assert obj.type_name == 'blob'
            indexed_object = models.Blob.get_from_cache_or_new(id=obj.id)
        indexed_object.save()
        indexed_object.add_repository(repo)
    else:
        if obj.type_name == 'tree':
            for name, mode, sha1 in obj.iteritems():
                # Default the type of the child object to a commit (a submodule)
                child_type = type_mapper.setdefault(sha1, 'commit')
                if child_type == 'tree':
                    child = models.Tree.get_from_cache_or_new(id=sha1)
                    child.add_parent(obj.id, name=name, mode=mode)
                elif child_type == 'blob':
                    child = models.Blob.get_from_cache_or_new(id=sha1)
                    child.add_parent(obj.id, name=name, mode=mode)
                else:
                    assert child_type == 'commit'
                    child = models.Commit.get_from_cache_or_new(id=sha1)
                    child.add_as_submodule_of(obj.id, name=name, mode=mode)
        elif obj.type_name == 'tag':
            # In dulwich, first entry is the child object.  In our custom parser,
            # it's None.
            _, child_id = obj.object
            child_type = type_mapper[child_id]
            child = _objectify(id=child_id, type=child_type)
            child.add_tag(obj.id)
        else:
            # Nothing to do for these.
            assert obj.type_name == 'blob' or obj.type_name == 'commit'

def _process_data(repo, uncompressed_pack, progress):
    logger.info('Creating objects for %s' % repo)
    type_mapper = {}
    for obj in uncompressed_pack.iterobjects():
        type_mapper[obj.id] = obj.type_name
        _process_object(repo=repo,
                        obj=obj,
                        progress=progress,
                        type_mapper=None,
                        iteration=1)
    logger.info('Constructed object type map of size %s (%d bytes) for %s' %
                (len(type_mapper), type_mapper.__sizeof__(), repo))

    logger.info('Now processing objects for %s' % repo)
    for obj in uncompressed_pack.iterobjects():
        _process_object(repo=repo,
                        obj=obj,
                        progress=progress,
                        type_mapper=type_mapper,
                        iteration=2)

def index_data(data, repo, is_path=False, unpack=False):
    if is_path:
        empty = not os.path.getsize(data)
    else:
        empty = data

    if empty:
        logger.info('No data to index')
        return
    objects_iterator = git_parser.ObjectsIterator(data, is_path, unpack)
    counter = {'count' : 0}
    def progress(object):
        counter['count'] += 1
        if not counter['count'] % 10000:
            check_for_die_file()
            logger.info('About to process object %d for %s (object is %s %s)' % (counter['count'],
                                                                                 repo,
                                                                                 object.type_name,
                                                                                 object.id))
    _process_data(repo, objects_iterator, progress)

def fetch_and_index(repo, recover_mode=False, packfile=None, batch=None, unpack=False):
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
        repo.dirty = True
        repo.save()
        models.flush()
        state = {}
        while True:
            data_path = fetch(repo, recover_mode=recover_mode,
                              packfile=packfile, batch=batch, state=state)
            index_data(data_path, repo, is_path=True, unpack=unpack)
            if not state.get('has_extra'):
                break
            else:
                logger.info('Still more remote heads, running again...')
        repo.count = repo.count_objects()
        repo.last_index = now
        repo.been_indexed = True
        repo.approved = True
        repo.dirty = False
        # Finally, clobber the old remote heads.
        repo.set_remote_heads(repo.new_remote_heads)
        repo.set_new_remote_heads([])
        repo.save()
        refresh_all_counts(all=False)
    except DeadRepo:
        logger.error('Marking %s as dead' % repo)
        repo.approved = 0
        repo.save()
    except KeyboardInterrupt:
        logger.info('^C pushed; exiting thread')
        raise
    except Exception, e:
        logger.error('Had a problem indexing %s: %s' % (repo, traceback.format_exc()))
    finally:
        repo.indexing = False
        repo.save()
        if not packfile and data_path:
            try:
                os.unlink(data_path)
            except IOError, e:
                logger.error('Could not remove tmpfile %s.: %s' % (data_path, e))
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

def index_all(last_index=None, threads=1, approved=None):
    repos = models.Repository.get_indexed_before(last_index, approved=approved)
    logger.info('About to index %d repos' % repos.count())
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

def refresh_all_counts(all=None):
    aggregator = models.Aggregate.get()
    aggregator.refresh_all_counts(all=all)
    aggregator.save()
    models.flush()
