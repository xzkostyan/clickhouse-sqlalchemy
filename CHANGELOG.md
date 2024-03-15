# Changelog

## [Unreleased]

## [0.3.1] - 2024-03-14
### Added
- ``SETTINGS`` clause. Pull request [#292](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/292) by [limonyellow](https://github.com/limonyellow).
- ``DISTINCT ON`` clause. Pull request [#293](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/293) by [aronbierbaum](https://github.com/aronbierbaum). Solves issue [#234](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/234).

### Fixed
- ``select(...).join(...)`` query generation. Pull request [#284](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/284) by [Net-Mist](https://github.com/Net-Mist). 
- [Native] Streaming results without explicitly setting `max_row_buffer`. Pull request [#287](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/287) by [akurdyukov](https://github.com/akurdyukov). Solves issue [#286](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/286).
- Username and password quoting for SQLAlchemy>=2.0.25. Pull request [#285](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/285) by [Net-Mist](https://github.com/Net-Mist). 
- ``match`` function case. Pull request [#283](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/283) by [bader-tayeb](https://github.com/bader-tayeb). 
- [alembic] Missing column on reflection. Pull request [#277](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/277) by [littlebtc](https://github.com/littlebtc). Solves issue [#280](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/280). 
- [alembic] Use `replace_existing=True` for alembic_version.version_num column. Pull request [#275](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/275) by [leemurus](https://github.com/leemurus). Solves issue [#288](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/288).
- [alembic] Table reflection for alembic version < 1.11. Solves issue [#274](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/274).

## [0.3.0] - 2023-11-06
### Changed
- Supported SQLAlchemy version is 2.0. Pull request [#256](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/256) by [PedroAquilino](https://github.com/PedroAquilino) and [#268](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/268) by [Net-Mist](https://github.com/Net-Mist). Solves issue [#259](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/259).

## [0.2.5] - 2023-10-29
### Added
- Table primary key reflection. Pull request [#265](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/265) by [akurdyukov](https://github.com/akurdyukov). Solves issue [#264](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/264).
- Python 3.11, 3.12 in Travis CI build matrix.

### Fixed
- Missing Array attributes. Pull request [#255](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/255) by [jonburdo](https://github.com/jonburdo). Solves issue [#254](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/254).
- DateTime and DateTime64 reflection. Solves issue [#148](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/148).
- Enum8 and Enum16 Unicode conversion. Pull request [#263](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/263) by [akurdyukov](https://github.com/akurdyukov). Solves issue [#192](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/192).
- [HTTP] Map type escaping. Pull request [#266](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/266) by [subkanthi](https://github.com/subkanthi). Solves issue [#148](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/148).
- Guess number of `_reflect_table` alembic argsPull request [#249](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/249) by [9bany](https://github.com/9bany). Solves issue [#250](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/250).

### Removed
- greenlet dependency. Pull request [#257](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/257) by [mrcljx](https://github.com/mrcljx).

## [0.2.4] - 2023-05-02
### Added
- [Native] DateTime('timezone'). Solves issue [#230](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/230).
- Async driver support via ``asynch``. Pull request [#214](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/214) by [randomowo](https://github.com/randomowo).
- ``ILIKE`` clause compilation. Pull request [#229](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/229) by [howbownotnow](https://github.com/howbownotnow).
- ``MATCH`` clause via ``Column.regexp_match``. Pull request [#221](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/221) by [TeddyCr](https://github.com/TeddyCr).

### Fixed
- Trigger ``after_drop`` and ``before_drop`` events while dropping table. Pull request [#246](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/246) by [vanchaxy](https://github.com/vanchaxy). Solves issue [#245](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/245).
- ``SAMPLE`` with ``ARRAY JOIN``. Pull request [#241](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/241) by [dpaluch-piw](https://github.com/dpaluch-piw). Solves issue [#45](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/45).
- Alembic comparators respect non-ClickHouse engines for multi-db. Pull request [#236](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/236) by [Skorpyon](https://github.com/Skorpyon).
- INNER/OUTER joins strictness on multiple joins.
- [Native] Handle quoted passwords. Solves issue [#185](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/185).

## [0.2.3] - 2022-11-24
### Added
- [U]Int128/256 types. Pull request [#184](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/184) by [akolov](https://github.com/akolov). Solves issue [#176](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/176).
- `WITH CUBE`/`WITH ROLLUP` modifiers for `GROUP BY`. Pull request [#210](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/210) by [reflection](https://github.com/reflection).
- Boolean type. Pull request [#213](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/213) by [AntonFriberg](https://github.com/AntonFriberg). Solves issue [#212](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/212).

### Fixed
- `FULL JOIN` sql generation. Pull request [#144](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/196) by [poofeg](https://github.com/poofeg). Solves issue [#193](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/193).
- Allow `CASE` without `ELSE` clause.
- Arrays compilation in `has*` functions. Solves issue [#205](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/205).
- Do not render foreign key constraints. Solves issue [#208](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/208).
- Inconsistency in `ischema_names` type converters. Pull request [#216](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/216) by [AntonFriberg](https://github.com/AntonFriberg). Solves issue [#215](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/215).
- LowCardinality and Nullable alembic autogeneration. Solves issue [#217](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/217).

## [0.2.2] - 2022-08-24
### Added
- Table and column comments creation and reflection. Solves issue [#149](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/149).

### Fixed
- [HTTP] Use query params to specify clickhouse data format. Pull request [#180](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/180) by [save-my-heart](https://github.com/save-my-heart).

### Changed
- Switch from nose test runner to pytest.

## [0.2.1] - 2022-06-13
### Fixed
- Add `supports_statement_cache = True`. Solves issue [#169](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/169).
- Add `cache_ok = True` for IP types.
- Mixed `text()` and pure strings in engine parameters handling. Solves issue [#173](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/173).
- Engine creation in alembic migrations.
- `CREATE` and `DROP` Materialized views. Solves issue [#177](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/177).

With changes from 0.1.9 and 0.1.10.

## [0.2.0] - 2022-02-20
### Added
- `LEFT ARRAY JOIN` clause. Pull request [#167](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/167) by [Fozar](https://github.com/Fozar).

### Changed
- Supported SQLAlchemy version is 1.4.

## [0.1.10] - 2022-06-06
### Added
- Documentation on Read the Docs: https://clickhouse-sqlalchemy.readthedocs.io

### Fixed
- ``AFTER`` clause rendering in ``ADD COLUMN``.
- Broken Materialized views creation via ``.create()``.
- Engine creation in migrations for alembic 1.6+. ``Engine`` is subclass of ``Constraint`` now.

## [0.1.9] - 2022-05-08
### Fixed
- ReplicatedReplacingMergeTree reflection. Solves issue [#164](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/164).
- Inline inserts with literal binds for alembic support.

## [0.1.8] - 2022-02-03
### Added
- Tuple and Map types. Pull request [#163](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/163) by [joelynch](https://github.com/joelynch).
- Materialized views alembic migrations autogeneration.

### Fixed
- Handle unsupported engines in table reflection.
- `EXISTS` and `DESCRIBE` table quoting.
- Default Unicode error handler to `replace`. Pull request [#166](https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/166) by [cwurm](https://github.com/cwurm).
- [Native] Inserts with `literal_column` values. Solves issue [#133](https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/133).
- Alembic nullable reflection.

### Changed
- Migrate from Travis CI to GitHub Actions.

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

[Unreleased]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.3.1...HEAD
[0.3.1]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.2.5...0.3.0
[0.2.5]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.2.4...0.2.5
[0.2.4]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.2.3...0.2.4
[0.2.3]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.2.2...0.2.3
[0.2.2]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.2.1...0.2.2
[0.2.1]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.9...0.2.0
[0.1.10]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.9...0.1.10
[0.1.9]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.8...0.1.9
[0.1.8]: https://github.com/xzkostyan/clickhouse-sqlalchemy/compare/0.1.7...0.1.8
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
