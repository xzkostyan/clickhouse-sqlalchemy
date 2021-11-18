# Changelog

## [Unreleased]

## [0.1.7] - 2021-11-18
### Added
- Support for distinctness comparisons (`is_distinct_from` / `isnot_distinct_from`). Pull request [#144](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/144) by [wlhjason](https://github.com/wlhjason). Solves issue [#143](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/143).
- [HTTP] Cert auth. Pull request [#128](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/128) by [evgG](https://github.com/evgG).
- [HTTP] Session factories. Pull request [#131](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/131) by [carlosefr](https://github.com/carlosefr).
- [HTTP] Raw engine execute. Pull request [#134](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/134) by [FishermanZzhang](https://github.com/FishermanZzhang).
- [HTTP] DateTime('timezone') support. Pull request [#141](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/141) by [lance-plusai](https://github.com/lance-plusai).
- Optional disabling engine reflection. Solves issue [#140](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/140).
- `AFTER` clause in `ALTER TABLE ... ADD COLUMN`. Pull request [#153](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/153) by [eivasch](https://github.com/eivasch).
-  Column default reflection from `DESCRIBE TABLE` default_expression. Pull request [#153](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/153) by [eivasch](https://github.com/eivasch).

### Fixed
- SAMPLE BY reflection. Solves issue [#127](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/127).
- [HTTP] `verify` behavior in requests. Pull request [#128](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/128) by [evgG](https://github.com/evgG).

## [0.1.6] - 2021-03-15
### Added
- [HTTP] Optional custom `requests.Session`. Pull request [#119](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/119) by [carlosefr](https://github.com/carlosefr).
- DateTime64 type. Pull request [#116](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/116) by [aamalev](https://github.com/aamalev).
- [HTTP] Keep HTTP connection open between queries. Pull request [#117](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/117) by [carlosefr](https://github.com/carlosefr).

### Fixed
- [HTTP] Don't lose information on unicode conversion. Pull request [#120](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/120) by [carlosefr](https://github.com/carlosefr).
- [HTTP] Fix server version parsing (non-numeric build). Pull request [#118](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/118) by [carlosefr](https://github.com/carlosefr).
- [HTTP] Handle nullable columns. Pull request [#121](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/121) by [carlosefr](https://github.com/carlosefr).
- Reflection for schema and views handling. Pull request [#125](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/125) by [hodgesrm](https://github.com/hodgesrm).
- Expressions reflection in *MergeTree engines. Solves issue [#123](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/123). 
- Columns compilation fix. Replace default dialect. Solves issue [#124](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/124).
- [HTTP] Proper handling `verify` flag when parsing DSN. Pull request [#126](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/126) by [yhvicey](https://github.com/yhvicey).

## [0.1.5] - 2020-12-14
### Added
- `MATERIALIZED` and `ALIAS` column options.
- `LIMIT BY` clause support. Pull request [#97](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/97) by [ods](https://github.com/ods).
- Basic engines reflection.
- `TTL` param for *MergeTree engines. Pull request [#111](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/111) by [aamalev](https://github.com/aamalev).

### Changed
- Session parametrization in tests.
- Exclude table name from `DEFAULT` column option.
- Allow multiple columns in `PARTITION BY`.
- Replace `uuid1` with `uuid4` for automatic query_id generation. Solves issue [#99](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/99).

### Fixed
- Remove table names during `JOIN` with`USING` clause.
- [Native] Case insensitive`VALUES` clause check for (%s)-templates.
- Render `sqlalchemy.Boolean` as `UInt8` instead of `BOOLEAN`. Solves issue [#93](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/93).
- Allow multiple columns in SummingMergeTree.
- Proper `JOIN` clause rendering. Solves issue [#108](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/108).

### Removed
- Drop Python 3.4 support due to urllib3 drop.

## [0.1.4] - 2020-04-30
### Fixed
- `if_exists` and `on_cluster` AttributeError on table drop. Pull request [#94](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/94) by [vmarkovtsev](https://github.com/vmarkovtsev).

## [0.1.3] - 2020-04-04
### Added
- Engines: ReplicatedReplacingMergeTree, VersionedCollapsingMergeTree, ReplicatedVersionedCollapsingMergeTree. Solves issue [#70](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/70).
- File engine. Pull request [#72](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/72) by [armymaksim](https://github.com/armymaksim).
- [HTTP] SSL certificates verification. Pull request [#75](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/75) by [NiyazNz](https://github.com/NiyazNz).
- Decimal type reflection. Pull request [#74](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/74) by [armymaksim](https://github.com/armymaksim).
- `ALTER DELETE`. Pull request [#81](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/81) by [sdrenn](https://github.com/sdrenn). Solves issue [#65](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/65).
- `ALTER UPDATE`. Pull request [#82](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/82) by [sdrenn](https://github.com/sdrenn). Solves issue [#65](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/65).
- [HTTP] Session tests, assorted fixes. Pull request [#73](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/73) by [hhell](https://github.com/hhell).
- `FINAL` clause. Pull request [#79](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/79) by [sdrenn](https://github.com/sdrenn).
- `CODEC` column option. Pull request [#89](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/89) by [athre0z](https://github.com/athre0z).
- Add literal binds support to IP types. Improve IP types `IN` and `NOT IN` comparators. Pull request [#91](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/91) by [AchilleAsh](https://github.com/AchilleAsh).

### Changed
- Minimal SQLAlchemy version supported is 1.3 now.
- Named arguments should go after positional in ReplacingMergeTree, SummingMergeTree. Pull request [#80](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/80) by [sdrenn](https://github.com/sdrenn).
- `sample` keyword argument changed to `sample_by` in *MergeTree.
- `version_col,` keyword argument changed to `version` in ReplacingMergeTree.

### Fixed
- [Native] Remove (%s)-templates from `VALUES` clause.
- [HTTP] `fetchone` elements order. Solves issue [#77](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/77).
- Fix ReplacingMergeTree creation with no version.
- License file now included in the package. Solves issue [#86](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/86).
- Columns are now prefixed with table name if necessary. Solves issue [#35](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/35) and issue [#87](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/87).
- Generate pure DLL without literals on Replicated tables creation.

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

[Unreleased]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.7...HEAD
[0.1.7]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.6...0.1.7
[0.1.6]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.5...0.1.6
[0.1.5]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.4...0.1.5
[0.1.4]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.3...0.1.4
[0.1.3]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.2...0.1.3
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
