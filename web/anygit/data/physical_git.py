import git.blob
import git.commit
import git.repo
import git.tag
import git.tree
import re

THE_ONE_REPO_PATH = '/home/greg/repositories/anygit/the-one-repo'
split_re = re.compile(r'[ \t]')

def classify(type):
    mapping = {'blob' : git.blob.Blob,
               'commit' : git.commit.Commit,
               'tag' : git.tag.Tag,
               'tree' : git.tree.Tree}
    return mapping[type]

class PhysicalRepo(git.repo.Repo):
    def get_raw_refs(self, remote=None):
        if not remote:
            result = self.git.for_each_ref('refs/remotes/')
        else:
            result = self.git.for_each_ref('refs/remotes/%s' % remote)
        return [split_re.split(ref)[0] for ref in result.split('\n')]

    def get_raw_objects_by_sha1(self, sha1):
        return [split_re.split(object)[0] for object in
                self.git.rev_list('--objects', sha1).split('\n')]

    def get_raw_objects_by_remote(self, remote):
        raw_refs = self.get_raw_refs(remote=remote)
        raw_objects = []
        for ref in raw_refs:
            raw_objects += self.get_raw_objects_by_sha1(ref)
        return raw_objects

    def rev_list(self):
        return self.git.rev_list('--all').split('\n')

    def get_object_type(self, sha1):
        return self.git.cat_file('-t', sha1)

    def id_to_physical_git_object(self, sha1):
        type = self.get_object_type(sha1)
        klass = classify(type)
        return klass(self, sha1)

    def ids_to_physical_git_objects(self, ids):
        return iter(self.id_to_physical_git_object(id) for id in ids)

    def git_objects(self):
        return [self.id_to_physical_git_object(id) for id in self.rev_list()]

THE_ONE_REPO = PhysicalRepo(path=THE_ONE_REPO_PATH)
