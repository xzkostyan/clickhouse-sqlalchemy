from sqlalchemy.orm import sessionmaker


def make_session(engine):
    Session = sessionmaker(bind=engine)

    return Session()
