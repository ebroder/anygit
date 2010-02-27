"""
The data models
"""

class Repository(object):
    def __init__(self, base_url, identifier):
        self.base_url = base_url
        self.identifier = identifier

class GitObject(object):
    def __init__(self, sha1):
        self.sha1 = sha1

class Blog(GitObject):
    @property
    def commits(self):
        pass

class Tree(GitObject):
    @property
    def commits(self):
        pass

class Commit(GitObject):
    @property
    def repositories(self):
        pass
