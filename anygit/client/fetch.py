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
        matching_commits = [c.id for c in models.Commit.find_matching(refs_dict.itervalues())]
        missing_commits = set(refs_dict.itervalues()) - set(matching_commits)
        return list(missing_commits)

    def get_parent(sha1):
        logger.debug('Called get parent on %s' % sha1)
        try:
            c = models.Commit.get(sha1)
        except exceptions.DoesNotExist:
            return []
        else:
            return [p.id for p in c.parents]

    retrieved_data = []
    def pack_data(data):
        logger.debug('Packing some data')
        retrieved_data.append(data)

    def progress(data):
        logger.debug('Received: %r' % data)
            
    graph_walker = object_store.ObjectStoreGraphWalker(repo.remote_heads,
                                                       get_parent)
    c = client.TCPGitClient(repo.host)
    c.fetch_pack(path=repo.path,
                 determine_wants=determine_wants,
                 graph_walker=graph_walker,
                 pack_data=pack_data,
                 progress=progress)

    return ''.join(retrieved_data)

def _process_data(data, callback, is_path):
    if is_path:
        pack_data = pack.PackData.from_path(data)
    else:
        file = StringIO.StringIO(data)
        length = len(data)
        pack_data = pack.PackData.from_file(file, length)
    uncompressed_pack = pack.Pack.from_objects(pack_data, None)
    for obj in uncompressed_pack.iterobjects():
        callback(obj)

def get_save_methods(repo):
    def create_object(obj):
        object_type = obj._type
        logger.debug('About to create %s %s' % (object_type, obj.id))
        if object_type == 'commit':
            c = models.Commit.get_or_create(id=obj.id)
        elif object_type == 'tree':
            t = models.Tree.get_or_create(id=obj.id)
        elif object_type == 'tag':
            t = models.Tag.get_or_create(id=obj.id)
        elif object_type == 'blob':
            b = models.Blob.get_or_create(id=obj.id)
        else:
            raise ValueEror('Unrecognized type %s' % object_type)        

    def index(obj):
        object_type = obj._type
        logger.debug('About to index %s %s' % (object_type, obj.id))
        if object_type == 'commit':
            c = models.Commit.get(id=obj.id)
            c.add_repository(repo)
            c.add_tree(obj.tree)
            c.add_parents(obj.parents)
        elif object_type == 'tree':
            t = models.Tree.get(id=obj.id)
            for _, _, sha1 in obj.iteritems():
                child = models.GitObject.get(sha1)
                child.add_commits(t.commits)
        elif object_type == 'tag':
            t = models.Tag.get_or_create(id=obj.id)
            raise UnimplementedError
        elif object_type == 'blob':
            pass
        else:
            raise ValueEror('Unrecognized type %s' % object_type)
    return create_object, index

def index_data(data, repo, is_path=False):
    if not data:
        logger.error('No data to index')
        return
    create_object, index = get_save_methods(repo)
    _process_data(data, callback=create_object, is_path=is_path)
    _process_data(data, callback=index, is_path=is_path)

def fetch_and_index(repo):
    data = fetch(repo)
    index_data(data, repo)

