from dulwich import client, object_store, pack
import logging
import StringIO

from anygit import models
from anygit.data import exceptions

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
        logger.debug('Called get parent on %s' % sha1)
        try:
            c = models.Commit.get_by_attributes(id=sha1, complete=True)
        except exceptions.DoesNotExist:
            return []
        else:
            return [p.id for p in c.parents]

    retrieved_data = []
    def pack_data(data):
        retrieved_data.append(data)

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

    return ''.join(retrieved_data)

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
        raise UnimplementedError

def index_data(data, repo, is_path=False):
    if not data:
        logger.error('No data to index')
        return
    objects_iterator = _get_objects_iterator(data, is_path)
    _process_data(repo, objects_iterator)

def fetch_and_index(repo):
    data = fetch(repo)
    index_data(data, repo)
    models.flush()
