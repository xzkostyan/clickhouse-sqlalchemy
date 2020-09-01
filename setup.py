import os
import re
from codecs import open

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))


with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


def read_version():
    regexp = re.compile(r'^VERSION\W*=\W*\(([^\(\)]*)\)')
    init_py = os.path.join(here, 'clickhouse_sqlalchemy', '__init__.py')
    with open(init_py, encoding='utf-8') as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1).replace(', ', '.')
        else:
            raise RuntimeError(
                'Cannot find version in clickhouse_sqlalchemy/__init__.py'
            )


dialects = [
    'clickhouse{}=clickhouse_sqlalchemy.drivers.{}'.format(driver, d_path)

    for driver, d_path in [
        ('', 'http.base:ClickHouseDialect_http'),
        ('.http', 'http.base:ClickHouseDialect_http'),
        ('.native', 'native.base:ClickHouseDialect_native')
    ]
]

setup(
    name='clickhouse-sqlalchemy',
    version=read_version(),

    description='Simple ClickHouse SQLAlchemy Dialect',
    long_description=long_description,

    url='https://github.com/xzkostyan/clickhouse-sqlalchemy',

    author='Konstantin Lebedev',
    author_email='kostyan.lebedev@gmail.com',

    license='MIT',

    classifiers=[
        'Development Status :: 4 - Beta',


        'Environment :: Console',


        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',


        'License :: OSI Approved :: MIT License',


        'Operating System :: OS Independent',


        'Programming Language :: SQL',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',

        'Topic :: Database',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],

    keywords='ClickHouse db database cloud analytics',

    packages=find_packages('.', exclude=["tests*"]),
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4',
    install_requires=[
        'sqlalchemy>=1.3,<1.4',
        'requests',
        'clickhouse-driver>=0.1.2',
        'ipaddress; python_version<"3.4"',
    ],
    # Registering `clickhouse` as dialect.
    entry_points={
        'sqlalchemy.dialects': dialects
    },
    test_suite='nose.collector',
    tests_require=[
        'nose',
        'sqlalchemy>=1.3,<1.4',
        'mock',
        'requests',
        'responses',
        'enum34; python_version<"3.4"',
        'parameterized'
    ],
)
