class CommonMixin(object):
    """Functionality common to all backends."""
    def __init__(self):
        super(CommonMixin, self).__init__()
        self._errors = {}

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        instance.save()
        return instance

    def __str__(self):
        return "%s: %s" % (self.type, self.name)

    def __repr__(self):
        return str(self)

    def error(self, attr, msg):
        self._errors.setdefault(attr, []).append(msg)

    def validate(self):
        pass


class CommonRepositoryMixin(CommonMixin):
    def validate(self):
        super(CommonRepositoryMixin, self).validate()
        if not self.name:
            self.error("name", "Must provide a name")
        if not self.url:
            self.error("url", "Must provide a url")


class CommonGitObjectMixin(CommonMixin):
    def validate(self):
        super(CommonGitObjectMixin, self).validate()
        if not self.name:
            self.error("name", "Must provide a name")


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
