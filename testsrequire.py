
tests_require = [
    'pytest',
    'sqlalchemy>=1.4.24,<1.5',
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
