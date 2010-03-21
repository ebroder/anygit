import logging

from anygit.data import exceptions
from anygit.data import physical_git
from anygit import models

logger = logging.getLogger('anygit.data.crawl')
repo = physical_git.THE_ONE_REPO

def add_repo(url):
    remote = physical_git.normalize_name(url)
    try:
        repo.add_remote(remote)
    except physical_git.GitCallError:
        logger.debug('Already had added %s' % url)

    try:
        repo_object = models.Repository.get(name=remote)
    except exceptions.DoesNotExist:
        logger.info('Time to create new repository %s' % remote)
        repo_object = models.Repository.create(name=remote, url=url)
    else:
        return True

def fetch_repo(remote):
    remote = physical_git.normalize_name(remote)
    repo.fetch(remote=remote)

def index_repo(remote):
    remote_name = physical_git.normalize_name(remote)
    remote = models.Repository.get(name=remote_name)
    for branch in repo.list_branches(remote):
        for commit in repo.list_commits(remote, branch):
            try:
                commit_object = models.Commit.get(sha1=commit)
            except exceptions.DoesNotExist:
                commit_object = models.Commit.create(sha1=commit)
            logger.info('Adding %s to the repositories containing %s' %
                            (remote, commit_object))
            commit_object.add_repository(remote)

            for blob in repo.list_blobs(commit):
                try:
                    blob_object = models.Blob.get(sha1=blob)
                except exceptions.DoesNotExist:
                    blob_object = models.Blob.create(sha1=blob)
                logger.info('Adding %s to the commits containing %s' %
                                (commit, blob_object))
                blob_object.add_commit(commit)
