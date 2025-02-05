class DatabaseException(Exception):
    def __init__(self, orig):
        self.orig = orig
        super().__init__(orig)

    def __str__(self):
        return f'Orig exception: {self.orig}'
