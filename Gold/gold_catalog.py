# Databricks notebook source
# Databricks notebook source
# Gold field catalog builder
# ─────────────────────────────────────────────────────────────────────────────
"""
Build the field catalog from gold metadata (the authored `catalog` block on each
table) and emit it as catalog.json alongside the gold tables.

The catalog is AUTHORED in gold_tables.json -- each table's `catalog.fields`
declares label + dataType per output column, and `catalog.hidden` lists columns
to omit. This module turns that into the final catalog the report editor consumes,
deriving the mechanical parts (which aggregations are valid, groupability, date
transforms) from dataType so they don't have to be hand-declared.

INSPECT MODE: each field carries a small `provenance` OBJECT so inspect mode can
explain it. The catalog stores only the minimal REFERENCES; all explanation prose
lives frontend-side (RESOLUTION_PROSE / METRIC_PROSE / etc.), keyed by these refs:

    provenance = {
      "type":     "native" | "resolved" | "metric" | "derived",   # always
      "strategy": "<resolve strategy name>",   # resolved fields only
      "agg":      "<metric agg>",              # metric fields only (its defining agg)
      "expr":     "<derive expr>",             # derived fields only
    }

Authored per field in the catalog block as:
    "sales_rep_e_email": { "label": "...", "dataType": "string",
                           "provenance": "resolved",
                           "strategy": "sales_rep_native_then_company_owner" }
`provenance` defaults to "native" when omitted (a raw source column).

This replaces the API's DESCRIBE + type-inference approach: types are declared
where the columns are born (gold), not guessed downstream. The catalog travels
with the data (written to the gold container) and refreshes on the same pipeline
run.
"""
import json

# semantic dataType -> (valid aggregations, groupable). Derived, not authored.
_AGG_RULES = {
    "currency":   (["sum", "avg", "min", "max"], False),
    "number":     (["sum", "avg", "min", "max", "count_distinct"], True),
    "percentage": (["avg", "min", "max"], False),
    "date":       (["min", "max", "count_distinct"], True),
    "string":     (["count_distinct"], True),
    "boolean":    (["count_distinct"], True),
}
_DATE_TRANSFORMS = ["day", "week", "month", "quarter", "year"]

_VALID_PROVENANCE = {"native", "resolved", "metric", "derived"}


def _build_provenance(table_name: str, key: str, spec: dict) -> dict:
    """Assemble the minimal provenance object from the field's catalog spec.
    type is required (defaults native); strategy/agg/expr are carried per type.
    The frontend holds the prose keyed by these; the catalog carries only refs."""
    prov_type = spec.get("provenance", "native")
    if prov_type not in _VALID_PROVENANCE:
        raise ValueError(
            f"{table_name}.{key}: invalid provenance '{prov_type}' "
            f"(must be one of {sorted(_VALID_PROVENANCE)})"
        )
    provenance = {"type": prov_type}
    if prov_type == "resolved" and "strategy" in spec:
        provenance["strategy"] = spec["strategy"]
    if prov_type == "metric" and "agg" in spec:
        provenance["agg"] = spec["agg"]
    if prov_type == "derived" and "expr" in spec:
        provenance["expr"] = spec["expr"]
    return provenance


def build_table_catalog(table_name: str, table_meta: dict) -> dict:
    """Build one table's catalog from its authored `catalog` block."""
    cat = table_meta.get("catalog")
    if not cat:
        return {"table": table_name, "fields": []}

    hidden = set(cat.get("hidden", []))
    fields = []
    for key, spec in cat.get("fields", {}).items():
        if key in hidden:
            continue
        dtype = spec.get("dataType", "string")
        aggs, groupable = _AGG_RULES.get(dtype, (["count_distinct"], True))

        field = {
            "key": key,
            "label": spec.get("label", key),
            "dataType": dtype,
            "aggregations": aggs,
            "groupable": groupable,
            "provenance": _build_provenance(table_name, key, spec),
        }
        if dtype == "date":
            field["transforms"] = _DATE_TRANSFORMS

        fields.append(field)

    # dimensions first, then measures; alpha by label within each
    fields.sort(key=lambda f: (not f["groupable"], f["label"].lower()))
    return {"table": table_name, "fields": fields}


def build_full_catalog(tables_meta: dict) -> dict:
    """Catalog for every EXPORTED table (export != false), keyed by table name.

    Only exported tables get a catalog -- internal dims (export:false) aren't
    queryable, so they don't belong in the editor's vocabulary.
    """
    out = {}
    for name, meta in tables_meta.items():
        if name.startswith("_") or not isinstance(meta, dict):
            continue
        if meta.get("export") is False:
            continue
        if "catalog" not in meta:
            continue
        out[name] = build_table_catalog(name, meta)
    return out


def write_catalog(tables_meta: dict, out_path: str):
    """Build the full catalog and write it as JSON to out_path (e.g. an ADLS
    path in the gold container). Called by gold_build after tables are written."""
    catalog = build_full_catalog(tables_meta)
    payload = json.dumps(catalog, indent=2)
    # dbutils is available in Databricks; fall back to open() elsewhere (tests).
    try:
        dbutils.fs.put(out_path, payload, overwrite=True)  # noqa: F821
    except NameError:
        with open(out_path, "w") as f:
            f.write(payload)
    print(f"✓ catalog.json written ({len(catalog)} tables) -> {out_path}")
    return catalog