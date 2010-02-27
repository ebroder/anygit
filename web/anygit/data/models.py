"""
The data models
"""

class Repository(object):
    def __init__(self, base_url, identifier):
        self.base_url = base_url
        self.identifier = identifier

class Object(object):
    def __init__(self, sha1):
        self.sha1 = sha1

class Blog(Object):
    @property
    def commits(self):
        pass

class Tree(Object):
    @property
    def commits(self):
        pass

class Commit(Object):
    @property
    def repositories(self):
        pass
