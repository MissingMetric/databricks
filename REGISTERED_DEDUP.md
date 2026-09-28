# Registered deduplication

Built from main after the earlier dedup framework was reverted. There is no
separate dedup config module or strategy dictionary. Business functions and their
English descriptions live in `Gold/gold_strategies.py`. Matchers use `DEDUP`;
selectors use `SELECTION`, both registered through the existing `_reg` mechanism.

## Configuration and rollout

The existing Supabase document remains `schema_version: 1`. Every table still
requires `resolve`. An optional `dedup` section is copied wholesale, never merged.
Omitting it preserves the existing structural metadata's legacy dedup settings.
Legacy `none` / `keep_first` remain available for existing configurations; they
do NOT provide the safeguards or audit trail of the new ordered-step contract.
No deployed client configuration is changed by this branch.

New example (illustrative fields must exist in the actual silver model):

```json
{
  "schema_version": 1,
  "tables": {
    "companies": {
      "resolve": {},
      "dedup": {
        "identity": ["source_platform", "source_account", "id"],
        "allow_identity_merge": true,
        "strategies": [
          {
            "id": "latest_version",
            "strategy": "same_source_identity",
            "selection": {
              "strategy": "ordered",
              "params": {
                "order_by": [{"field": "modified_date", "direction": "desc", "nulls": "last"}],
                "on_tie": "fail"
              }
            }
          },
          {
            "id": "business_key",
            "strategy": "normalized_key",
            "params": {
              "scope_by": ["source_platform", "source_account"],
              "keys": [{"field": "name", "normalize": "company_name"}, {"field": "domain", "normalize": "lower_trim"}],
              "must_agree": ["owner_id"]
            },
            "selection": {
              "strategy": "ordered",
              "params": {"order_by": [{"field": "modified_date", "direction": "desc"}, {"field": "id", "direction": "asc"}]}
            }
          }
        ]
      }
    }
  }
}
```

Include all actual tables in the client's document. Do not apply company-name
merging just because two names look similar. Email can be shared; SKU can be
reused between accounts and variants; a timestamp is not a unique order key.
`must_agree` treats null versus a value as a conflict. Blank matching keys never
match. Normalization is explicit: exact, lower_trim, or company_name.

Example order-copy step (row identity must include platform/account/order/line):

```json
{
  "id": "integration_copy",
  "strategy": "external_reference",
  "params": {
    "unit_by": ["source_platform", "source_account", "order_id"],
    "from": {"where": {"source_platform": "cin7", "source_account": "erp"}, "reference_field": "ecommerce_order_id"},
    "to": {"where": {"source_platform": "shopify", "source_account": "store"}, "identity_field": "order_id"},
    "cardinality": "one_to_one"
  },
  "selection": {"strategy": "referenced_record"},
  "on_ambiguity": "fail"
}
```

Reverse endpoints to reverse integration direction. A missing target is retained;
multiple copies referring to one target fail (split shipments may be legitimate).
All lines of a losing order representation are excluded together. A reference
must agree on every source line, including null versus non-null. This policy
does NOT establish that the referenced order's basket is complete. Enable only
where the target is contractually authoritative; don't apply to invoices,
fulfillments, returns or payments merely because they reference an order.

## Engine changes

`run_gold` still runs dedup, resolve, metrics, enrich/derive with the same barriers.
It now passes the table name to `run_dedup`. `run_dedup` dispatches new configured
steps through `_run_registered_dedup`; legacy behavior is untouched.

The wrapper validates matcher/selector row preservation, declared contracts,
one group per member and one existing winning member per group. It captures
decisions before exclusions. It collapses identical full payload occurrences
with a count, then fails if final declared row identities are still duplicated.
Null/blank identity fields fail. No arbitrary UUID is added to hide bad grain.
Rows must use Spark-groupable scalar/struct/array payloads; MapType columns need
canonical upstream normalization (map grouping is not supported).

Matching precedes selection. `ordered` supports asc/desc, null placement and
`{"field":"source_platform","prefer":["hubspot","cin7"],"unlisted":"last"}`.
Ties between different top candidates fail. External-unit matching currently
allows only `referenced_record`; ordering line-level values to select an order
would be unsafe. There is no automatic transitive match closure.

## Evidence and storage

Catalog row_provenance publishes configured steps and only the used definitions.
Gold rows contain compact `__mm_dedup_source` / `__mm_dedup_evidence` with run,
table, scoped identity, step history and identical-occurrence count. They do not
contain all discarded snapshots or all earlier members of a final group.

`ctx['dedup_audits'][table]` records each step's input snapshot, occurrence count,
retained/excluded decision, matcher and selector references and actual values.
The build writes it before replacing gold to private client storage:
`audit/dedup/<run_id>/<table>/decisions`. Aliases are written beside it.
These contain personal/business data: restrict storage access and define retention.
Never include audit paths in ordinary gold table discovery or public URLs.

Publishing gold tables/catalog remains the existing non-atomic multi-file process.
Writing audits first prevents missing audit dependencies, but does not implement
an atomic dataset release. API on-demand audit browsing is not part of this branch.

## IDs and references

Original IDs are preserved. `ctx['dedup_aliases'][table][tuple(identity_fields)]`
contains source_identity -> canonical_identity JSON mappings. Earlier mappings
are composed forward when later steps use the same identity grain; all intermediate
decisions remain in audit. Source-version selection doesn't change business IDs.
Multiple cross-ID steps must use the same mapping grain; mixed-grain chains are
rejected rather than leaving stale aliases.
Whole-order mappings are at order grain, NOT fabricated line-to-line matches.

`lookup_dedup_alias(df, ctx, table, field_mapping, output)` is an explicit helper
for registered resolvers; mapping keys are the complete target bare identity fields,
values are columns on the referencing frame. It returns JSON
so the resolver can extract desired fields and record original/canonical values.
The engine does not silently rewrite foreign keys. Existing ID-lookup resolvers
must be adapted to use the helper before enabling merges of IDs they reference.
Cross-ID matchers require `allow_identity_merge: true` as explicit client opt-in
after this reference audit. The default is false; it is not an automatic proof
that every external consumer has been migrated.
Do not enable cross-ID dedup on referenced dimensions until those consumers have
been audited. Name-based matching is not a substitute for this referential check.

Mappings are run-specific, not a persistent surrogate-key allocation service.
Canonical identities can change when winner data/config changes. Cross-run stable
business identities, fuzzy matching, quarantine UI, line-set completeness comparison
and field-by-field merging are deliberately not inferred.

## Verification

`python -m unittest discover -s tests -p "test_*.py" -v`

`python tests/run_registered_dedup_spark.py -v` (PySpark and Java17; Windows local
runner uses jdk4py). Tests cover versions, occurrence counts, tenant/account scope,
selection ties, missing identities, owner conflicts, shared emails, blank SKUs,
whole order baskets, absent references, one-to-many integration mistakes, inconsistent
line references, multiple steps, rerun determinism and empty input.

Local Spark tests do not certify Databricks Spark Connect or ADLS publication.
Run these scenarios in a non-production Databricks job before adopting the config.

### Business pressure-test interpretation

| Scenario | Intended result |
| --- | --- |
| Repeated source delivery | Collapse identical payloads; retain occurrence count |
| Updated company record | Select latest declared source modification |
| Two different latest versions with equal timestamps | Fail; no arbitrary winner |
| Same ID in different source accounts | Preserve both |
| Same company business key, different owners (including missing owner) | Fail when owner is protected |
| Two contacts sharing a family/office email | Protect name or another identity attribute; conflict fails |
| Reused SKU for different product variants | Protect variant; conflict fails |
| Missing SKU/email/name | Do not group blank matching keys |
| Order copied between systems, multiple lines each | Retain all lines from referenced representation |
| Reference points to an order not loaded yet | Retain the referencing order |
| Split fulfillments or several source orders point at one target | Fail the one-to-one policy; require a different business policy |
| Different references on lines of the same order | Fail before exclusion |
| A maps to B, then B maps to C | Compose final aliases to C; retain step decisions |
| Input order changes between runs | Same result for the same input/config |

These are synthetic correctness tests, not a production-volume benchmark. The
runner deliberately performs expensive validation actions and does not claim
optimal throughput. Profile larger client extracts before scaling. No example
policy authorizes merging separate purchases, invoices, returns, partial shipments,
product variants or people simply because a few descriptive fields coincide.
