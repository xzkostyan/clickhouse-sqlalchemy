class DatabaseException(Exception):
    def __init__(self, orig):
        super().__init__(orig)
        self.orig = orig

    def __str__(self):
        return f"Original exception: {self.orig}"
