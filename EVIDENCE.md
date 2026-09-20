# Evidence-enabled gold catalog (v2)

## Contract

Strategy definitions are maintained with their decorators in `Gold/gold_strategies.py`.
Edit the implementation and English description/outcomes together. Increment the
decorator's `version` when semantics change. The registry publishes only strategies
referenced by the merged client configuration, including platform overrides.

Each strategy application emits an output-specific `<target>_source` and
`<target>_evidence`. Existing legacy source columns are retained for compatibility.
Evidence is JSON text (not a Spark struct) so heterogeneous platform strategies
union safely and export through Parquet/DuckDB without losing explicit nulls.
The envelope contains `strategy`, `field`, `run_id`, `processed_at`, `source`,
`result`, and `inputs`. Strategy IDs include their stage and version.

Resolver evidence captures lookup values before scratch columns are dropped.
Metric evidence is compact: it records the current row's input, partition key,
ordering input when configured, and the result—not every row in the partition.
Dedup evidence describes the retained row, not discarded candidates. Enrichment
records its own join and carries the source field's complete evidence under
`inputs.upstream_decision`; resolver dependency evidence can be nested similarly.
Derived fields record their configured inputs, including explicit nulls.

The client catalog is `{schema_version: 2, run_id, tables, strategies}`. Each field
has explicit evidence column mappings. Table metadata authorizes the fields usable
for evidence queries, record display, and record identity. The API does not infer
column names from suffixes or contain business-field dictionaries.

## Compatibility and rollout

1. Deploy the API compatibility reader and new evidence endpoint first. It serves
   old catalogs normally and reports evidence unavailable without inventing facts.
2. Deploy the Next.js client. Existing reports remain readable before a gold rebuild.
3. Sync the changed gold notebooks and run the tests below in a development workspace.
4. Run the gold build for a development client. No cloud configuration files need
   changes for the supplied configuration; actual merged overrides drive the catalog.
5. Refresh both API data and catalog, refresh the report, then compare selected
   values with evidence totals before rolling out to other clients.

No deployment or Databricks run is performed by the local implementation.
Downloaded configuration files are preserved as test fixtures, not deployment sources.
The source's existing attribution outcomes and ambiguous-match behavior are not
changed. English definitions describe their actual limitations.

## Validation

Local: `python -m unittest discover -s tests -v` (catalog and syntax tests).
API: install DuckDB/FastAPI in a test environment and run `python -m unittest test_evidence -v`
from the API repository. Those tests use synthetic data and an in-memory database.

Spark runtime validation is still required in a development Databricks workspace.
Run `%run ./gold_strategies`, then exercise direct/fallback/unresolved resolution,
null platform dispatch, empty input, metrics, and left/full enrichment. Compare
original output values and row counts, parse each evidence JSON, verify explicit
null inputs, and confirm the JSON survives a Parquet round trip. Validate enriched
account-owner evidence retains its original owner-ID lookup beneath the join.
Do not treat local catalog/SQL tests as a substitute for this Spark check.
