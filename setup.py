from setuptools import setup
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='clickhouse-sqlalchemy',
    version='0.0.1',

    description='Simple ClickHouse SQLAlchemy Dialect using HTTP interface',
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


        'Topic :: Database',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],

    keywords='ClickHouse db database cloud analytics',

    packages=[
        'clickhouse_sqlalchemy'
    ],
    package_dir={
        'clickhouse_sqlalchemy': 'src',
    },
    install_requires=[
        'sqlalchemy',
        'requests',
    ],

    # Registering `clickhouse` as dialect.
    entry_points={
        'sqlalchemy.dialects': [
            'clickhouse=clickhouse_sqlalchemy.base',
        ]
    },

    test_suite='nose.collector',
    tests_require=[
        'nose',
        'SQLAlchemy>=1.0',
        'mock==1.0.1',
        'requests',
        'responses'
    ],
)
