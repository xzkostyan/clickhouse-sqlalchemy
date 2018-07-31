# Changelog

## [Unreleased]

## [0.0.7] - 2018-07-31
### Fixed
- Handling NULL values in arrays as strings. Pull request [#23](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/23) by [Joozty](https://github.com/Joozty).

## [0.0.6] - 2018-07-20
### Added
- Schema names as databases support. Pull request [#16](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/16) by [AchilleAsh](https://github.com/AchilleAsh).
- DateTime type.
- Reflection Array, FixedString and Nullable types.

### Fixed
- Pip install in editable mode.

### Removed
- Python 3.3 support.

## [0.0.5] - 2017-11-06
### Added
- `JOIN` clause support via `tuple_()`.
- Version detection in setup.py.

### Changed
- Using native driver parameters substitution via pyformat.

### Fixed
- Binary mod operation compilation issue [#8](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/8).

## [0.0.4] - 2017-10-03
### Added
- `SAMPLE` clause.
- Code coverage.
- Additional engines: AggregatingMergeTree, GraphiteMergeTree, ReplacingMergeTree, ReplicatedMergeTree,
ReplicatedCollapsingMergeTree, ReplicatedAggregatingMergeTree, ReplicatedSummingMergeTree. Distributed,
Log, TinyLog, Null.
- Changelog.
- Lambda functions generation.

## [0.0.3] - 2017-07-16
### Added
- `extract('year', x)` alias to `toYear(x)`/`toMonth(x)`/`toDayOfMonth(x)`. Pull request [#2](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/2) by [gribanov-d](https://github.com/gribanov-d).
- External tables in native interface support.
- Nullable type support in `CREATE TABLE`.
- `WITH TOTALS` clause support for `GROUP BY`.
- Passing settings via `execution_options`.

### Changed
- Native driver elements reverse order issue fixed.
- Fixed `count(expr)` rendering. Pull request [#3](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/3) by [gribanov-d](https://github.com/gribanov-d).
- Fixed empty string parse error over HTTP. Pull request [#5](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/5) by [gribanov-d](https://github.com/gribanov-d).
- Nested Array type generation fixed.
- Structure refactored. 

## [0.0.2] - 2017-06-23
### Added
- Travis CI.
- flake8 syntax check.
- Native (TCP) interface support.
- Python 3.3+ support.

### Changed
- `ELSE` clause is required in `CASE`.

## 0.0.1 - 2017-03-30
### Added
- HTTP/HTTPS protocol support.
- Python 2.7 support.
- Engine declaration support in `__table_args__`
- `DROP TABLE IF EXISTS` clause.
- Automatic registering as dialect `clickhouse://`.
- Chunked `INSERT INTO` in one request.
- Engines: MergeTree, CollapsingMergeTree, SummingMergeTree, Buffer, Memory. 

[Unreleased]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.7...HEAD
[0.0.7]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.6...0.0.7
[0.0.6]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.5...0.0.6
[0.0.5]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.4...0.0.5
[0.0.4]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.2...0.0.3
[0.0.2]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.1...0.0.2
