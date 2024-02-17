
tests_require = [
    'pytest',
    'sqlalchemy>=2.0.0,<2.1.0',
    'greenlet>=2.0.1',
    'alembic',
    'requests',
    'responses',
    'parameterized'
]

try:
    from pip import main as pipmain
except ImportError:
    from pip._internal import main as pipmain

pipmain(['install'] + tests_require)
