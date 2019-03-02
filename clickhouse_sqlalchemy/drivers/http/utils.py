import codecs


def unescape(value):
    return codecs.escape_decode(value)[0].decode('utf-8', errors='replace')


def parse_tsv(line):
    if line and line[-1] == b'\n':
        line = line[:-1]
    return [unescape(x) if x != b'\\N' else None for x in line.split(b'\t')]
