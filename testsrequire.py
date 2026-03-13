
import subprocess
import sys


tests_require = [
    'pytest',
    'pytest-asyncio',
    'sqlalchemy>=2.0.0,<2.1.0',
    'greenlet>=2.0.1',
    'alembic',
    'requests',
    'responses',
    'parameterized'
]

subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + tests_require)
