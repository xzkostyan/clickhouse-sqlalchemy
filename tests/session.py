from sqlalchemy import create_engine
from sqlalchemy.dialects import registry

from src.session import make_session


registry.register("clickhouse", "src.drivers.http.base", "dialect")

uri = 'clickhouse://default:@localhost:8123/default'

session = make_session(create_engine(uri))
