# Composable gold resolution and evidence

## Ownership

Supabase's existing `client_configs_by_slug.resolution_config` is now the complete
resolver document, not an override. It requires `schema_version: 1` and a `tables`
object explicitly covering every gold table (use `resolve: {}` where appropriate).
Missing config, unknown/missing tables and legacy strategy specs fail closed.
Shared ADLS `Gold/gold_tables.json` still supplies table structure, catalog labels,
metrics, enrichments and derives. Its resolve sections are replaced, never merged.
This change moves **resolver policy**, not every gold-stage configuration, to Supabase.

Use `config/development.resolution_config.json` as the development client's complete
resolver configuration. The example preserves the former platform distinction:
native rep only by default; native then company-owner for Shopify.
If fallback should apply on every platform, put both strategies in the default chain.

## Strategy contract

A chain uses strings or objects with `id`, `strategy` and `params`.
Each platform variant supplies a full replacement chain. First success wins;
only unresolved rows enter the next step. Blank strings count as missing.
All unresolved outputs are null, not the literal identity "unresolved".

Resolver functions return their input rows plus `__mm_candidate`, a struct of
string fields: value, status, reason and inputs (JSON text preserving nulls).
They do not assign output columns or decide precedence. The engine validates
row preservation using multiset comparisons and checks the candidate contract.
These checks incur Spark jobs and shuffles; benchmark with representative data
before production. Conflicting directory matches fail the build instead of
choosing an arbitrary record or silently falling through.
Identical lookup payloads are collapsed safely.

Each strategy declares its row inputs and default column parameters in its
decorator. Unknown params/strategies/inputs and dependency cycles are rejected.
Resolver targets are topologically ordered, including dependencies in platform
variants. Directory inputs always come from the post-dedup, native-prefixed
stage-one snapshot, never partially resolved tables.

Current resolver pieces return string identities/emails. Adding numeric resolver
pieces will require extending the explicit candidate type contract; no implicit
conversion is used to claim numeric metrics are supported here.

## Evidence and catalog

The engine writes one `<target>_source` and `<target>_evidence` pair.
Obsolete `company_e_source`, `company_e_mismatch`, and `sales_rep_e_source`
side effects are not emitted by resolvers.
Resolver evidence version 2 includes:

- field, run_id, processed_at, result, status
- strategy and winner_step (null if exhausted)
- source (winning reason, or unresolved)
- attempts, in execution order: step, strategy, status, reason, inputs

Attempt inputs are JSON strings inside the envelope to preserve heterogeneous
schemas across strategies/platforms. Explain parses them, including nested
company-decision evidence. Later, unattempted steps do not appear on a record.
The field catalog's `provenance.resolution` lists the full default/platform chains.
Shared English definitions remain next to implementations in decorators and
are published once per client, only for configured strategies.
Catalog schema version remains 2; resolver evidence has its own version.

Other stages retain their existing evidence contracts. Metric evidence records
the row's configured inputs/partition/result, not all rows in the aggregate.
Enrichment carries source evidence in `inputs.upstream_decision`.

The API still groups by winning strategy/source using catalog-mapped fields.
Exhausted chains form an unresolved group; corrupt/missing evidence is separate.
Explain shows configured precedence, actual attempts, values used and raw details.
This is not a new field-level redaction policy: existing evidence visibility
limits described in the previous review still apply.

## Development rollout (manual; not deployed by this change)

1. Replace the development client's `resolution_config` JSON in Supabase with
   the example above. Use the actual table behind the existing view; this repo
   does not contain that database schema, so no guessed SQL migration is supplied.
2. Sync all changed Gold notebooks, including the new `gold_resolver_config.py`.
   Do not mix old combined strategies with the new engine.
3. Run `Gold/gold_evidence_smoke.py` on a development Databricks cluster. It uses
   synthetic in-memory data, no cloud reads/writes. Verify performance on a
   representative development snapshot before production.
4. Deploy the API and frontend changes together with the new data contract.
5. Run the development gold build; refresh both API data and catalog caches,
   then refresh the report. Compare totals and inspect direct/fallback/unresolved
   records. Gold exports are still the existing non-atomic write process.
6. If reverting, restore the previous code AND config/data snapshot together.
   There is intentionally no legacy resolver configuration compatibility.

## Local verification

- Databricks repo: `python -m unittest discover -s tests -v`.
- API repo: `python -m unittest test_evidence -v` (synthetic DuckDB).
- Frontend: TypeScript check and the existing Node regression tests.
- Spark smoke tests must run in Databricks when PySpark/Java are unavailable locally.

Local tests are not a substitute for executing the Spark smoke notebook.
