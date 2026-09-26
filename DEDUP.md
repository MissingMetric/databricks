# Client-owned deduplication

## Deploy as one version

The existing Supabase `resolution_config` column now requires **schema_version 2**.
Every table entry requires `resolve` AND `dedup`; optional `identity_links` describe
foreign IDs that need remapping. There are no inherited dedup defaults or deep merges.
`config/development.resolution_config.json` is the complete replacement document.
Do not paste only its dedup block or mix v1 configuration with these notebooks.

1. Sync all changed Gold notebooks, including `gold_dedup.py` and
   `gold_dedup_config.py`. Run `gold_dedup_smoke.py` in a development cluster.
2. Replace development's Supabase configuration with the version-2 document.
3. Run the development build. Inspect its canonical tables, aliases, match edges,
   conflicts and occurrence counts before trusting reports.
4. Refresh API data and catalog together, then refresh the frontend.

This implementation does not push, deploy, update Supabase, delete CRM records,
or modify source data. Revert code and config together if rolling back.

## Development policies

- Sales reps: exact copies, then the latest modification within platform + rep ID.
- Companies: the same version handling, then normalized-name matching within the
  platform. Owner IDs must agree, including null vs non-null. The smallest scoped
  ID in lexical order wins when no selection preference is configured. This is
  an explicit TEST-DATA policy, not a recommendation to merge client companies
  by name automatically. The two supplied Iron House Gym IDs consolidate to
  `316522121920`; both IDs remain in the alias mapping.
- Orders: exact copies only at LINE grain. Distinct line IDs remain distinct even
  when product, price and quantity are identical. Conflicting versions fail rather
  than guessing. Cross-identity consolidation is disabled.
- Company owner IDs are remapped through the sales-rep aliases before prefixing
  and resolution. Additional foreign-key relationships must be explicitly declared.

The development fixture has only `source_platform`. For a client with multiple
accounts/stores per platform, ingest an account identifier and include it in BOTH
`scope` and `identity`. Platform alone is not sufficient isolation in that case.
Matching requires scoped, non-null source identities. Metadata's `grain_pk` is a
declared column even when an older `base_columns` list omitted it; runtime validation
also checks the actual dataframe before execution.

## Matching and source revisions

All policies are validated before source loading and again against runtime columns.
Names are PRE-PREFIX source names (company `name`, not `company_e_name`).

- `none`: explicit preservation; no alias mapping is produced.
- `exact_record`: groups by full serialized native payload and records occurrence
  counts. Hash collisions cannot combine records; equality is on the full payload.
- `same_source_identity`: ranks source versions using configured `order_by` fields.
  Different payloads tied at the top rank stop the build. They are never selected
  by a random partition order. Requires exact_record first.
- `unique_business_key`: matches complete keys within scope. Supported normalization:
  exact, lower_trim, company_name (same rules as the company resolver).
- `explicit_identity_mapping`: client-approved direct aliases to a chosen existing
  identity. Missing endpoints or multiple requested canonical IDs in a component
  fail. Alias chains should be flattened to their canonical target in config.
- `external_reference`: matches reference_field to target_field between explicitly
  bound from_scope/to_scope accounts. Automatic consolidation requires one-to-one
  matches. An absent counterpart does NOT remove the available record. Flag mode
  permits reviewing non-one-to-one matches without consolidating them.
- `composite_fingerprint`: uses equality of complete configured key fields; defaults
  to flag. It does not construct a full order basket. A basket fingerprint must be
  supplied as a canonical source field with multiplicity, tax, currency, timestamps,
  discounts and other required semantics defined by the client.

Business matchers live in `DEDUP_MATCHERS`; they produce evidence-bearing links,
not survivor dataframes. Their English definitions live once in
`gold_dedup_config.py` and only configured definitions are published in the catalog.
The source-version/exact stages are structural prerequisites, not entity-match rules.
Matching steps inspect all current source representatives. Their list order is NOT
first-success fallback. All merge edges are reconciled together; flag edges never
join components. Transitive groups are validated against protected fields as a whole.
The distributed graph is bounded to 64 iterations; non-convergence fails closed.

## Selection and conflicts

Each business rule chooses `action: merge` or `action: flag`. Merge additionally
requires table-level `allow_entity_merge: true`; leave it false for order lines.
Explicit canonical mappings have priority over the selection policy. Otherwise:

```json
"selection": {
  "prefer_values": [{"field": "source_platform", "values": ["shopify", "cin7"]}],
  "order_by": [{"field": "modified_date", "direction": "desc"}]
}
```

Preferences and ordering are applied in declaration order; unknown sources and null
ordering values sort last. A final scoped-identity lexical tie-break is reproducible,
not evidence of a more correct business record. Empty ordering is permitted only as
an explicit acceptance of that deterministic choice. Do not use timestamps from two
systems as comparable revisions without checking their semantics.

`conflict_fields` must agree across the ENTIRE proposed component. Null vs populated
is treated as disagreement, not implicit permission to fill values. `on_conflict`:

- `fail` (default): stops before gold export. Inspect `ctx['dedup_conflicts'][table]`.
- `retain`: publishes all current entities in that component, with conflict flags;
  it does NOT silently select a winner. Later name resolution may still fail on
  those ambiguous entities. This option does not implement unresolved attribution.

Source-version ties, missing IDs, invalid aliases and non-one-to-one automatic
external references always fail. Quarantine and fuzzy matching are not implemented.
Protected fields are client-owned; unsafe/overbroad keys cannot be made reliable
merely by specifying a deterministic tie-breaker.

## Canonical identity and references

The selected source identity becomes the canonical identity, stored as a JSON object
of the configured scoped identity fields. Every original source ID has a mapping.
Repeated runs with unchanged data/config are deterministic. There is NOT yet a durable
cross-run canonical-ID allocation service: a new preferred record or changed policy
can change the winner. Use explicit aliases to pin identity where required.

`identity_links` bind a foreign field to another table's identity and scope:

```json
{"field": "owner_id", "table": "sales_reps", "key": "id",
 "scope": {"source_platform": "source_platform"}}
```

After every table is deduplicated, declared foreign IDs are remapped before native
prefixing and resolver execution. Unknown references are preserved; there is no
silent drop. Each rewrite has `__mm_identity_link_<n>` JSON evidence. Mapping scope
must bind the complete identity, and links to tables with `none` are rejected.

## Audit and Explain

Every surviving row has `__mm_dedup_source` and `__mm_dedup_evidence`. The catalog's
`row_provenance` includes the complete policy, identity links and audit location.
Company-name resolver evidence carries the selected company's nested dedup decision.
Existing Explain's raw/nested evidence reader can display that trace.

Artifacts in `ctx`:

- `dedup_aliases[table]`: source_identity, canonical_identity, selected_record, conflict.
- `dedup_audits[table]['records']`: every distinct original payload, occurrence count,
  canonical payload, selection/supersession/consolidation decision and conflict flag.
- `dedup_audits[table]['matches']`: proposed pairs, step/strategy, action and actual keys.
- `dedup_audits[table]['conflicts']`: conflicting group members and original payloads.

The build writes these to `audit/dedup/<run_id>/<table>/`, outside the API's gold
table discovery, BEFORE replacing gold outputs. They contain full source payloads:
keep this area restricted to pipeline/admin identities, not public serving storage.
No new API endpoint or UI for browsing excluded records is included. That is a
separate access-controlled feature, not automatic exposure of this audit folder.
Gold publication itself remains the existing non-atomic process.

## Validation and remaining boundaries

Local configuration/catalog tests: `python -m unittest discover -s tests -v`.
Local Spark 3.5 + Java 17: `python tests/run_dedup_spark.py -v`.
Databricks smoke: run `Gold/gold_dedup_smoke.py`.
Local test dependencies live only in the ignored virtual environment.

The runtime uses native Spark joins/windows plus local checkpoints (classic Spark
cluster required). It avoids collecting entire datasets; conflict checks and graph
iterations cause shuffles/actions. Benchmark representative client data before
production; executor loss can invalidate a local checkpoint and require rerunning
the build. Large duplicate groups deserve review rather than unlimited grouping.

Order-header canonicalization, coordinated header/line source selection, split or
consolidated orders, fuzzy matching, quarantine, durable canonical-ID allocation,
and an excluded-record review UI are intentionally not implemented here. Do not
enable entity merging on the current order-line fact to simulate order deduplication.
