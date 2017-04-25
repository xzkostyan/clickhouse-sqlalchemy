from __future__ import absolute_import, division, print_function

from sqlalchemy.orm import sessionmaker


def make_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()
