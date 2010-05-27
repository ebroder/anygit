#!/usr/bin/python
import os
import subprocess
import sys
import tempfile

from dulwich import pack

DIR = os.path.abspath(os.path.dirname(__file__))
UNPACK_DIR = os.path.join(DIR, '../../tmp/unpack')
GIT_CMD = os.path.join(DIR, '../../pkgs/git/git')

types = {'t' : 'tree', 'b' : 'blob', 'c' : 'commit', 'a' : 'tag'}

class Finished(Exception):
    pass


class ObjectsIterator(object):
    def __init__(self, data, is_path, unpack):
        if not is_path:
            assert not unpack
        self.data = data
        self.is_path = is_path
        self.unpack = unpack

        if not unpack:
            if is_path:
                pack_data = pack.PackData.from_path(data)
            else:
                file = StringIO.StringIO(data)
                length = len(data)
                pack_data = pack.PackData.from_file(file, length)
            self.uncompressed_pack = pack.Pack.from_objects(pack_data, None)

    def iterobjects(self):
        if self.unpack:
            # Initialize a new repo and unpack into there.  Should use our
            # patched unpacker, which prints out parseable data.  For best
            # performance, make UNPACK_DIR be on a tmpfs.
            assert self.is_path
            unpack_dir = tempfile.mkdtemp(prefix='unpack_', suffix='.git', dir=UNPACK_DIR)
            subprocess.check_call([GIT_CMD, 'init', '--bare', unpack_dir])
            p = subprocess.Popen([GIT_CMD, 'unpack-objects'],
                                 cwd=unpack_dir,
                                 stdin=file(self.data),
                                 stdout=subprocess.PIPE)
            return parse(p.stdout)
        else:
            return self.uncompressed_pack.iterobjects()


class GitObject(object):
    """A git object, copying the interface of dulwich objects."""
    def __init__(self, id):
        self.id = id

    def __str__(self):
        return '%s: %s' % (type(self).__name__, self.id)


class Tree(GitObject):
    _type = 'tree'

    def __init__(self, id, children):
        super(Tree, self).__init__(id)
        self.children = children

    def iteritems(self):
        return iter(self.children)


class Tag(GitObject):
    _type = 'tag'

    def __init__(self, id, child_sha1):
        super(Tree, self).__init__(id)
        self.child_sha1 = child_sha1

    def get_object(self):
        return (None, self.child_sha1)


class Commit(GitObject):
    _type = 'commit'

    def __init__(self, id, tree, parents):
        super(Commit, self).__init__(id)
        self.tree = tree
        self.parents = parents


class Blob(GitObject):
    _type = 'blob'    

def get_next_len(f):
    t = f.read(1)
    if not t:
        raise Finished
    type = types[t]
    space = f.read(1)
    assert space == ' '
    accum = 0
    while True:
        n = f.read(1)
        if n != '\0':
            accum = int(n) + 10 * accum
        else:
            break
    return type, accum

def null_split(s):
    for i, char in enumerate(s):
        if char == '\0':
            return s[:i], s[i+1:]
    else:
        raise ValueError('No null byte found in %s' % s)

def grab_sha1(s, encoded=False):
    if encoded:
        return s[:40], s[40:]
    else:
        return s[:20].encode('hex'), s[20:]

def grab_mode(s):
    return s[:5], s[6:]

def parse(f):
    try:
        while True:
            type, accum = get_next_len(f)
            sha1 = f.read(20).encode('hex')
            if type == 'tree':
                data = f.read(accum)
                children = []
                while data:
                    mode, data = grab_mode(data)
                    filename, data = null_split(data)
                    child_sha1, data = grab_sha1(data, encoded=False)
                    children.append((filename, mode, child_sha1))
                yield Tree(sha1, children)
            elif type == 'tag':
                data = f.read(accum)
                assert data[:7] == 'object '
                child_sha1, _ = grab_sha1(data[7:], encoded=True)
                yield Tag(sha1, child_sha1)
            elif type == 'commit':
                tree = None
                parents = []
                data = f.read(accum)
                while data[:6] != 'author':
                    if data[:5] == 'tree ':
                        assert tree is None
                        tree, data = grab_sha1(data[5:], encoded=True)
                    else:
                        assert data[:7] == 'parent '
                        child_sha1, data = grab_sha1(data[7:], encoded=True)
                        parents.append(child_sha1)
                    # Slurp a newline
                    assert data[0] == '\n'
                    data = data[1:]
                yield Commit(sha1, tree, parents)
            else:
                assert type == 'blob'
                yield Blob(sha1)
    except Finished:
        print 'Completed'

if __name__ == '__main__':
    for type, sha1, extra in parse(sys.stdin):
        print type, sha1, extra
