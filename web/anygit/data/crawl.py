#!/usr/bin/env python

import physical_git
import models

def index_repo(url):
    repo = physical_git.THE_ONE_REPO
    remote = repo.add_remote(url)
    repo.fetch(remote=remote)
    repo_object = models.Repository(name=remote, url=url)
    repo_object.save()
    for branch in repo.list_branches(remote):
        for commit in repo.list_commits(remote, branch):
            commit_object = models.Commit(sha1=commit, repositories=[remote])
            commit_object.save()
            for blob in repo.list_blobs(commit):
                blob_object = models.Blob(sha1=blob, commits=[commit])
                blob_object.save()
