import datetime
from dulwich import client, object_store, pack
import glob
import logging
import os
import re
import subprocess
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

def call(*args, **kwargs):
    if 'send' in kwargs:
        send = kwargs.pop('send')
    else:
        send = None
    kwargs.setdefault('stdout', subprocess.PIPE)
    p = subprocess.Popen(*args, **kwargs)
    out, _ = p.communicate(None)
    if p.returncode:
        raise RuntimeException('Problem running %s %s' % (args, kwargs))
    return out

def _sha1_iterator(data_path):
    def process(path):
        prefix, suffix = path.split('/')[-2:]
        return prefix + suffix
    return (process(path) for path in glob.glob(os.path.join(data_path, 'objects/*/*')))

def _get_type(data_path, sha1):
    return call(['git', 'cat-file', '-t', sha1], cwd=data_path).strip('\n')

tree_re = re.compile('^(\d+) (\w+) (\w+)\t(.*)$')
def _tree_items(data_path, sha1):
    for line in call(['git', 'cat-file', '-p', sha1], cwd=data_path).split('\n'):
        if not line:
            continue
        match = tree_re.search(line)
        yield match.group(1), match.group(2), match.group(3), match.group(4)

commit_tree_re = re.compile('^tree (\w+)$')
commit_parent_re = re.compile('^commit (\w+)$')
def _get_parents_and_tree(data_path, sha1):
    lines = call(['git', 'cat-file', '-p', sha1], cwd=data_path).split('\n')
    tree = commit_tree_re.search(lines[0]).group(1)
    parents = []
    for line in lines[1:]:
        match = commit_parent_re.search(line)
        if match:
            parents.append(match.group(1))
        else:
            break
    return parents, tree

def fetch(repo, recover_mode=False, discover_only=False, get_count=False, packfile=None):
    """Fetch data from a remote.  If recover_mode, will fetch all data
    as if we had indexed none of it.  Otherwise will do the right thing
    with the pack protocol.  If discover_only, will fetch no data."""
    if not packfile:
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
    else:
        destfile_name = packfile

    git_dir = tempfile.mkdtemp()
    call(['git', 'init', '--bare'], cwd=git_dir)
    call(['git', 'unpack-objects'], stdin=open(destfile_name), cwd=git_dir)
    return git_dir

def _objectify(id, type):
    mapper = {'blob' : models.Blob,
              'tree' : models.Tree,
              'commit' : models.Commit,
              'tag' : models.Tag}
    return mapper[type].get_from_cache_or_new(id=id)

def _process_data(repo, data_path, progress):
    logger.info('Now processing objects for %s' % repo)
    for sha1 in _sha1_iterator(data_path):
        progress(sha1)
        type = _get_type(data_path, sha1)
        if type == 'tree':
            indexed_object = models.Tree.get_from_cache_or_new(id=sha1)
            for mode, child_type, child_sha1, name in _tree_items(data_path, sha1):
                # Default the type of the child object to a commit (a submodule)
                if child_type == 'tree':
                    child = models.Tree.get_from_cache_or_new(id=child_sha1)
                    child.add_parent(indexed_object, name=name, mode=mode)
                elif child_type == 'blob':
                    child = models.Blob.get_from_cache_or_new(id=child_sha1)
                    child.add_parent(indexed_object, name=name, mode=mode)
                else:
                    assert child_type == 'commit'
                    child = models.Commit.get_from_cache_or_new(id=child_sha1)
                    child.add_as_submodule_of(indexed_object, name=name, mode=mode)
                child.save()
        elif type == 'commit':
            parents, tree = _get_parents_and_tree(data_path, sha1)

            indexed_object = models.Commit.get_from_cache_or_new(id=sha1)
            indexed_object.add_parents(parents)
            indexed_object.save()

            child = models.Tree.get_from_cache_or_new(id=tree)
            child.add_commit(indexed_object)
            child.save()
        elif type == 'tag':
            child, child_id = obj.get_object()
            child_type = child._type

            indexed_object = models.Tag.get_from_cache_or_new(id=sha1)
            indexed_object.set_object_id(child_id)
            indexed_object.save()

            child = _objectify(id=child_id, type=child_type)
            child.add_tag(indexed_object)
            child.save()
        elif type == 'blob':
            indexed_object = models.Blob.get_from_cache_or_new(id=sha1)
        else:
            raise ValueError('Unrecognized git object type %s' % type)

    logger.info('Cleaning objects for %s' % repo)
    type_mapper = {}
    for id, type in type_mapper.iteritems():
        dirty = _objectify(id=id, type=type)
        dirty.mark_dirty(False)
        dirty.save()

def index_data(data_path, repo):
    counter = {'count' : 0}
    def progress(object):
        counter['count'] += 1
        if not counter['count'] % 10000:
            check_for_die_file()
            logger.info('About to process object %d for %s (object is %s %s)' % (counter['count'],
                                                                                 repo,
                                                                                 object._type,
                                                                                 object.id))
    _process_data(repo, data_path, progress)

def fetch_and_index(repo, recover_mode=False, packfile=None):
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
        data_path = fetch(repo, recover_mode=recover_mode, packfile=packfile)
        index_data(data_path, repo)
        repo.last_index = now
        repo.been_indexed = True
        # Finally, clobber the old remote heads.
        repo.set_remote_heads(repo.new_remote_heads)
        repo.set_new_remote_heads([])
        repo.save()
    except Exception, e:
        logger.error('Had a problem: %s' % traceback.format_exc())
    finally:
        if not packfile and data_path:
            try:
                pass
                # os.unlink(data_path)
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
