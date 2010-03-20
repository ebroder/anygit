import itertools
import logging
import re

import django.core.exceptions
from django.db import models

from anygit import data
import anygit.settings
import anygit.data.models
import anygit.data.exceptions


partial_sha1_re = re.compile('^[a-fA-F0-9]*$')

def escape(value):
    """Escape a value for use in a query."""
    # TODO: make this actually escape
    if not partial_sha1_re.search(value):
        raise ValidationError('Invalid value')
    return value

def frontend_class(klass):
    lookup = {Repository : data.models.Repository,
              Blob : data.models.Blob,
              Tree : data.models.Tree,
              Commit : data.models.Commit}
    return lookup[klass]

def frontify(iterator):
    def convert(instance):
        return frontend_class(instance.__class__)(backend=instance)
    return itertools.imap(convert, iterator)

class CustomModel(models.Model):
    @classmethod
    def get(cls, **kwargs):
        try:
            return cls.objects.get(**kwargs)
        except django.core.exceptions.ObjectDoesNotExist, e:
            raise data.exceptions.DoesNotExist(str(e))

    class Meta:
        abstract = True

class Repository(CustomModel):
    name = models.CharField(max_length=255, unique=True, primary_key=True)
    url = models.CharField(max_length=255, unique=True)

class GitObject(CustomModel):
    sha1 = models.CharField(max_length=255, unique=True, primary_key=True)

    @staticmethod
    def lookup_by_sha1(sha1, partial=False):
        if partial:
            blobs = Blob.objects.filter(sha1__startswith=sha1).iterator()
            trees = Tree.objects.filter(sha1__startswith=sha1).iterator()
            commits = Commit.objects.filter(sha1__startswith=sha1).iterator()
        else:
            blobs = Blob.objects.filter(sha1=sha1).iterator()
            trees = Tree.objects.filter(sha1=sha1).iterator()
            commits = Commit.objects.filter(sha1=sha1).iterator()
        results = itertools.chain(blobs, trees, commits)
        return frontify(results)

    class Meta:
        abstract = True


class Blob(GitObject):
    _commits = models.ManyToManyField('Commit')

    @property
    def commits(self):
        return frontify(self._commits.iterator())

    @property
    def commit_names(self):
        def get_name(commit):
            return commit.name
        return itertools.imap(get_name, self._commits.objects.iterator())

    def add_commit(self, sha1):
        if isinstance(sha1, str):
            commit = Commit.get(sha1=sha1)
        self._commits.add(commit)

    @property
    def repositories(self):
        return Repository.objects.filter(commit__blob__sha1__exact=self.sha1)


class Tree(GitObject):
    commits = models.ManyToManyField('Commit')

    @property
    def commits(self):
        return frontify(self._commits.objects.iterator())

    @property
    def commit_names(self):
        def get_name(commit):
            return commit.name
        return itertools.imap(get_name, self._commits.objects.iterator()).iterator()

    def add_commit(self, sha1):
        if isinstance(sha1, str):
            commit = Commit.get(sha1=sha1)
        self._commits.add(commit)

    @property
    def repositories(self):
        return frontify(Repository.objects.
                        filter(commit__trees__sha1__exact=self.sha1).iterator())

class Commit(GitObject):
    _repositories = models.ManyToManyField(Repository)

    @property
    def repositories(self):
        return frontify(self._repositories.iterator())

    @property
    def repository_names(self):
        def get_name(repositorie):
            return repository.name
        return itertools.imap(get_name, self._repositories.iterator())

    def add_repository(self, remote):
        if isinstance(remote, str):
            repository = Repository.get(name=remote)
        self._repositories.add(repository)
