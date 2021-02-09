import codecs


def unescape(value, use_surrogates=False):
    # Surrogates keep strings reversible to what's really in the database.
    errors = 'surrogateescape' if use_surrogates else 'replace'

    # Things like '\0' arrive as '\\0' and must be escaped explicitly.
    return codecs.escape_decode(value)[0].decode('utf-8', errors=errors)


def parse_tsv(line, use_surrogates=False):
    if line and line[-1] == b'\n':
        line = line[:-1]
    return [
        (unescape(x, use_surrogates) if x != b'\\N' else None)
        for x in line.split(b'\t')
    ]
