
import six


class DatabaseException(Exception):
    def __init__(self, orig):
        self.orig = orig
        super(DatabaseException, self).__init__()

    def __str__(self):
        text = 'Orig exception: {}'.format(self.orig)

        if six.PY3:
            return six.text_type(text)

        else:
            return six.text_type(text).encode('utf-8')
