class Error(Exception):
    pass

class DoesNotExist(Error):
    pass

class NotUnique(Error):
    pass

class ValidationError(Error):
    pass
