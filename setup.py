from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


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
    version='0.0.3',

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
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',

        'Topic :: Database',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],

    keywords='ClickHouse db database cloud analytics',

    packages=[
        p.replace('src', 'clickhouse_sqlalchemy')
        for p in find_packages(exclude=['tests'])
        if p.startswith('src')
    ],
    package_dir={
        'clickhouse_sqlalchemy': 'src',
    },
    install_requires=[
        'six',
        'sqlalchemy',
        'requests',
        'clickhouse_driver>=0.0.5'
    ],

    # Registering `clickhouse` as dialect.
    entry_points={
        'sqlalchemy.dialects': dialects
    },

    test_suite='nose.collector',
    tests_require=[
        'nose',
        'SQLAlchemy>=1.0',
        'mock',
        'requests',
        'responses',
        'enum34'
    ],
)
