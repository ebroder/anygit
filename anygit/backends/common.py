import hashlib
import re
import urlparse

from anygit.data import exceptions

def sha1(string):
    return hashlib.sha1(string).hexdigest()

class CommonMixin(object):
    """Functionality common to all backends."""
    def __new__(cls, *args, **kwargs):
        instance = super(CommonMixin, cls).__new__(cls)
        instance._errors = {}
        return instance

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        instance.save()
        return instance

    @classmethod
    def get_or_create(cls, **kwargs):
        try:
            return cls.get_by_attributes(**kwargs)
        except exceptions.DoesNotExist:
            return cls.create(**kwargs)

    def error(self, attr, msg):
        self._errors.setdefault(attr, []).append(msg)

    def validate(self):
        pass


class CommonRepositoryMixin(CommonMixin):
    @classmethod
    def create(cls, url):
        id = sha1(url)
        return super(CommonRepositoryMixin, cls).create(id=id, url=url)

    def __str__(self):
        return "Repository: %s" % self.id
    __repr__ = __str__

    def validate(self):
        super(CommonRepositoryMixin, self).validate()
        if not self.id:
            self.error("id", "Must provide an id")
        if not self.url:
            self.error("url", "Must provide a url")

    @property
    def _parsed_url(self):
        stripped_url = re.sub('^[^:]*:', '', self.url)
        return urlparse.urlparse(stripped_url)

    @property
    def host(self):
        return self._parsed_url.netloc

    @property
    def path(self):
        return self._parsed_url.path


class CommonRemoteHeadMixin(CommonMixin):
    pass


class CommonGitObjectMixin(CommonMixin):
    def __str__(self):
        return "%s: %s" % (self.type, self.id)
    __repr__ = __str__

    def validate(self):
        super(CommonGitObjectMixin, self).validate()
        if not self.id:
            self.error("id", "Must provide an id")


class CommonBlobMixin(CommonGitObjectMixin):
    pass


class CommonTreeMixin(CommonGitObjectMixin):
    pass


class CommonCommitMixin(CommonGitObjectMixin):
    pass


class CommonTagMixin(CommonGitObjectMixin):
    def validate(self):
        super(CommonTagMixin, self).validate()
        if not self.commit:
            self.error("commit", "Must provide a commit")
