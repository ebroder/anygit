import logging
from anygit.data import physical_git
from anygit import models

logger = logging.getLogger('anygit.data.cleanup')
repo = physical_git.THE_ONE_REPO

class Error(Exception):
    pass

def dedup():
    def unique(l):        
        return list(set(l))

    for git_object in models.GitObject.all():
        logger.debug('Starting on %s' % git_object)
        if git_object.type == 'commit':
            orig = git_object.repositories
            git_object.repositories = unique(orig)
            if len(git_object.repositories) != len(orig):
                logger.error('Looks like we eliminated dups in %s (original repositories: %s' % (git_object, orig))
            if 'None' in git_object.repositories:
                git_object.delete()
            else:
                git_object.save()
        elif git_object.type == 'blob':
            orig = git_object.commits
            git_object.commits = unique(orig)
            if len(git_object.commits) != len(orig):
                logger.error('Looks like we eliminated dups in %s (original commits: %s' % (git_object, orig))
            if 'None' in git_object.commits:
                git_object.delete()
            else:
                git_object.save()
        else:
            raise Error('Unsupported object %s' % git_object)

def delete_all():
    for git_object in models.GitObject.all():
        git_object.delete()
