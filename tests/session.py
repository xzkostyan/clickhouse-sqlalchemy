from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects import registry

registry.register("clickhouse", "src.drivers.http.base", "dialect")

uri = 'clickhouse://default:@localhost:8123/default'

Session = sessionmaker(bind=create_engine(uri))

session = Session()
