from sqlalchemy import types


class Point(types.UserDefinedType):
    __visit_name__ = "point"

class Ring(types.UserDefinedType):
    __visit_name__ = "ring"

class Polygon(types.UserDefinedType):
    __visit_name__ = "polygon"

class MultiPolygon(types.UserDefinedType):
    __visit_name__ = "multipolygon"
