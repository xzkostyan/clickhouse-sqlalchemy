import codecs


def unescape(value):
    return codecs.escape_decode(value)[0].decode('utf-8')


def parse_tsv(line):
    if line[-1] == '\n':
        line = line[:-1]
    return [unescape(value) for value in line.split('\t')]
