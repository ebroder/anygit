#!/usr/bin/python
import sys

types = {'t' : 'tree', 'b' : 'blob', 'c' : 'commit', 'a' : 'tag'}

class Finished(Exception):
    pass

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
                    children.append((child_sha1, filename, mode))
                yield type, sha1, (children, )
            elif type == 'tag':
                data = f.read(accum)
                assert data[:7] == 'object '
                child_sha1, _ = grab_sha1(data[7:], encoded=True)
                yield type, sha1, (child_sha1, )
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
                yield type, sha1, (tree, parents)
            else:
                assert type == 'blob'
                yield type, sha1, ()
    except Finished:
        print 'Completed'

if __name__ == '__main__':
    for type, sha1, extra in parse(sys.stdin):
        print type, sha1, extra
