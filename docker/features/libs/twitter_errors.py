class TwitterRateLimitError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class TwitterUserNotFoundError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class TwitterUserInvalidOrExpiredToken(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class TwitterUserAccountLocked(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class TwitterPageDoesnotExist(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class TwitterUnknownError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)