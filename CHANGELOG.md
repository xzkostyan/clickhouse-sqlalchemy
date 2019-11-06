# Changelog

## [Unreleased]

## [0.1.2] - 2019-11-06
### Fixed
- Generic `Table` reflection in case of `autoload=True`.
- [HTTP] Fix `get_schema_names`.

## [0.1.1] - 2019-10-31
### Fixed
- Set default strictness to `INNER` for join.
- MergeTree `PARTITION BY` clause now accepts functions.

## [0.1.0] - 2019-10-31
### Added
- Enum without explicit size. Pull request [#69](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/69) by [ei-grad](https://github.com/ei-grad).
- [Native] Passing all parameters to `clickhouse-driver` by using querystring in DSN.
- [Native] Bypass `types_check` to `clickhouse-driver` via `execution_options`.
- [Native] Store rowcount on `INSERT`. Returning rows count from `INSERT FROM SELECT` is not supported.
- Python 3.8 in Travis CI build matrix.
- Assorted fixes and improvements from a downstream internal fork. Pull request [#62](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/62) by [hhell](https://github.com/hhell).
- LowCardinality type modifier. Pull request [#59](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/59) by [hhell](https://github.com/hhell).
- [Native] IPv4 and IPv6 types. Pull request [#52](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/52) by [AchilleAsh](https://github.com/AchilleAsh).
- Nested types. Pull request [#49](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/49) by [aCLr](https://github.com/aCLr).
- Support for `FULL` parameter in `JOIN` rendering. Pull request [#50](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/50) by [PiwikPRO](https://github.com/PiwikPRO).
- `ARRAY JOIN` clause. Pull request [#44](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/44) by [aCLr](https://github.com/aCLr).

### Fixed
- [Native] Allow empty auth in DSN.
- [Native] Allow default secure port.
- Engine columns bool comparison errors.
- [HTTP] `UnicodeDecodeError`. Pull request [#51](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/51) by [aminought](https://github.com/aminought).

### Changed
- Ability to use custom partition key and primary keys differs from sorting keys for *MergeTree. Pull request [#48](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/48) by [aCLr](https://github.com/aCLr).
- Cursor performance increased in`fetchmany` and `fetchall`.
- Add dependencies environment markers in setup.py. Pull request [#58](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/58) by [nitoqq](https://github.com/nitoqq).
- Joins support refactor. Added `strictness` (`ANY`/`ALL`), `distribution` (`GLOBAL`) parameters. Pull request [#53](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/53) by [aCLr](https://github.com/aCLr).

## [0.0.10] - 2019-02-05
### Added
- Self-signed certificate support. Pull request [#46](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/46) by [rrockru](https://github.com/rrockru).
- UUID type. Pull request [#41](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/41) by [BolshakovNO](https://github.com/BolshakovNO).
- Enum type reflection. Pull request [#33](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/33) by [sochi](https://github.com/sochi).
- Decimal type. Pull request [#38](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/38) by [nikitka](https://github.com/nikitka).

### Changed
- Minimal SQLAlchemy version supported is 1.2 now.

### Fixed
- Handling additional column`comment_expression` in `DESCRIBE TABLE` results during reflection (in ClickHouse server >= 18.15).

## [0.0.9] - 2019-01-21
### Added
- `ON CLUSTER` clause in `CREATE TABLE`, `DROP TABLE`.

### Fixed
- Raw connection execute. Pull request [#40](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/40) by [AchilleAsh](https://github.com/AchilleAsh).
Solves issue [#39](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/39).

## [0.0.8] - 2018-11-25
### Added
- Streaming support via `yield_per`.
- Python 3.7 in Travis CI build matrix.

### Fixed
- Handling boolean values of `secure` query parameter of database url.
- `Cursor.__iter__` now conforms with PEP 479. Pull request [#29](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/29) by [khvn26](https://github.com/khvn26).
Solves issue [#27](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/27).
- Multiprocessing/asyncio pickling issues. Pull request [#36](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/36) by [jhirniak](https://github.com/jhirniak).
Solves issue [#13](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/13).

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

[Unreleased]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.2...HEAD
[0.1.2]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.10...0.1.0
[0.0.10]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.9...0.0.10
[0.0.9]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.8...0.0.9
[0.0.8]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.7...0.0.8
[0.0.7]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.6...0.0.7
[0.0.6]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.5...0.0.6
[0.0.5]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.4...0.0.5
[0.0.4]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.2...0.0.3
[0.0.2]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.0.1...0.0.2
