import logging
import physical_git
import models

logger = logging.getLogger('anygit.data.crawl')
repo = physical_git.THE_ONE_REPO

def add_repo(url):
    remote = repo.add_remote(url)
    try:
        repo_object = models.Repository.get(remote)
    except models.NoSuchObject:
        logger.info('Time to create new repository %s' % remote)
        repo_object = models.Repository(name=remote, url=url)
        repo_object.save()

def fetch_repo(remote):
    remote = physical_git.normalize_name(remote)
    repo.fetch(remote=remote)

def index_repo(remote):
    remote = physical_git.normalize_name(remote)
    for branch in repo.list_branches(remote):
        for commit in repo.list_commits(remote, branch):
            try:
                commit_object = models.Commit.get(commit)
            except models.NoSuchObject:
                commit_object = models.Commit(sha1=commit,
                                              repositories=[remote])
                commit_object.save()
            else:
                if remote not in commit_object.repositories:
                    logger.info('Adding %s to the repositories containing %s' %
                                (remote, commit_object))
                    commit_object.repositories.append(remote)
                    commit_object.save()
                else:
                    logger.info('We already knew %s contained %s' %
                                (remote, commit_object))

            for blob in repo.list_blobs(commit):
                try:
                    blob_object = models.Blob.get(blob)
                except models.NoSuchObject:
                    blob_object = models.Blob(sha1=blob, commits=[commit])
                    blob_object.save()
                else:
                    if commit not in blob_object.commits:
                        logger.info('Adding %s to the commits containing %s' %
                                    (commit, blob_object))
                        blob_object.commits.append(commit)
                        blob_object.save()
                    else:
                        logger.info('We already knew %s contained %s' %
                                    (commit, blob_object))
