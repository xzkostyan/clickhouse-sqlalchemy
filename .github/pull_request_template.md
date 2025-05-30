<!--
Link to relevant issue(s) or previous PR(s), one per line. Use "fixes" to automatically close the related issue.
-->

- fixes #<issue number>

<!--
Ensure each step in the "CONTRIBUTING.rst" file is completed by adding an "x" inside each box below.

If only the documentation is changed, the checklist below may be removed.
-->

Checklist:

- [ ] Add tests demonstrating the correct behavior of the change. All tests should pass.
- [ ] Add/Update relevant docs in the code and in the `docs` directory.
- [ ] Ensure the PR doesn't contain the code non-conformant with project formatting and linting rules.
- [ ] Run `flake8`/`ruff` and fix issues.
- [ ] Run `pytest` no tests failed. See the [development](https://clickhouse-sqlalchemy.readthedocs.io/en/latest/development.html) section.
