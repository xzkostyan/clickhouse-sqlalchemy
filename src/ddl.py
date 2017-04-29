from sqlalchemy.sql.ddl import (
    SchemaDropper as SchemaDropperBase, DropTable as DropTableBase
)


class DropTable(DropTableBase):
    def __init__(self, element, if_exists=False):
        self.if_exists = if_exists
        super(DropTable, self).__init__(element)


class SchemaDropper(SchemaDropperBase):
    def __init__(self, dialect, connection, if_exists=False, **kwargs):
        self.if_exists = if_exists
        super(SchemaDropper, self).__init__(dialect, connection, **kwargs)

    def visit_table(self, table, drop_ok=False, _is_metadata_operation=False):
        self.connection.execute(DropTable(table, if_exists=self.if_exists))
