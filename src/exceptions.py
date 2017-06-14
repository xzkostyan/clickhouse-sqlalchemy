
import six


class DatabaseException(Exception):
    def __init__(self, orig):
        self.orig = orig
        super(DatabaseException, self).__init__()

    def __str__(self):
        text = 'Orig exception: {}'.format(self.orig)

        if six.PY3:
            return str(text)

        else:
            return unicode(text).encode('utf-8')
