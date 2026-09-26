# Databricks notebook source
# Databricks notebook source
# Gold field catalog builder
# ─────────────────────────────────────────────────────────────────────────────
"""Generate a client-specific v2 catalog from client gold configuration.

Output: {schema_version, run_id, tables, strategies}. English descriptions live
with strategy decorators, not in this module or in frontend business dictionaries.
Only used strategies are published, once per client. Field provenance is derived
from the actual client resolver/metric/enrich/derive configuration, including
platform-specific chains. Evidence columns are explicitly mapped per output field.
The authored catalog still controls labels, data types and editor visibility.
"""
import json
import copy

# COMMAND ----------
# MAGIC %run ./gold_resolver_config
# COMMAND ----------

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
    build_full_catalog replaces these hints with actual strategy references."""
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


def build_full_catalog(tables_meta: dict, strategies=None, schema_columns=None, run_id=None) -> dict:
    """Catalog for every EXPORTED table (export != false), keyed by table name.

    Only exported tables get a catalog -- internal dims (export:false) aren't
    queryable, so they don't belong in the editor's vocabulary.
    """
    # Definitions are passed from the same registry that executes the strategies.
    # Never infer the applied strategy from the authored catalog prose.
    strategies = strategies if strategies is not None else globals().get("STRATEGY_DEFINITIONS", {})
    strategies = copy.deepcopy(strategies)
    for name, description in {**DEDUP_DEFINITIONS, "pipeline": "Applies client-owned matching, conflict and survivor policies; preserves source records and aliases in the run audit."}.items():
        ref = f"dedup.{name}@1"
        strategies[ref] = {"id": ref, "name": name, "stage": "dedup", "version": 1,
                          "description": description, "outcomes": {"retained": "This is a retained reporting record; inspect the run audit for exclusions and conflicts."}}
    used = set()

    def reference(stage, name):
        matches = [k for k, v in strategies.items() if v["stage"] == stage and v["name"] == name]
        if len(matches) != 1:
            raise ValueError(f"Expected one active definition for {stage}.{name}")
        used.add(matches[0])
        return matches[0]

    def field_provenance(table, key, trail=()):
        if (table, key) in trail:
            raise ValueError(f"Cyclic field provenance: {table}.{key}")
        tm = tables_meta[table]
        refs, definition = [], {}
        if key in tm.get("resolve", {}):
            spec = tm["resolve"][key]
            variants = resolver_variants(spec)
            def publish(steps):
                published = []
                for step in steps:
                    ref = reference("resolve", step["strategy"])
                    defaults = {k: v["default_field"] for k, v in strategies[ref].get("inputs", {}).items()
                                if isinstance(v, dict) and "default_field" in v}
                    published.append({**copy.deepcopy(step), "strategy": ref,
                                      "params": {**defaults, **step["params"]}})
                return published
            chain = publish(variants[None])
            platforms = {p: publish(steps) for p, steps in variants.items() if p is not None}
            refs = list(dict.fromkeys(step["strategy"] for steps in [chain, *platforms.values()] for step in steps))
            definition = {"type": "resolved", "resolution": {"policy": "first_success",
                "strategies": chain, "by_platform": platforms}}
        else:
            for stage, section, op in [("metric", "metrics", "agg"), ("derive", "derive", "expr")]:
                spec = next((s for s in tm.get(section, []) if s["name"] == key), None)
                if spec:
                    refs = [reference(stage, spec[op])]
                    definition = {"type": "metric" if stage == "metric" else "derived",
                                  op: spec[op], "calculation": copy.deepcopy(spec)}
                    break
            if not refs:
                for spec in tm.get("enrich", []):
                    source = next((s for s, t in spec.get("bring", {}).items() if t == key), None)
                    if source is not None:
                        origin = field_provenance(spec["from"], source, (*trail, (table, key)))
                        refs = [reference("enrich", spec.get("strategy", "left_join_bring"))]
                        definition = {"type": "enriched", "origin": {"table": spec["from"],
                            "field": source, "provenance": origin}, "join": {"on": spec["on"],
                            "source_on": spec.get("source_on", spec["on"])}}
                        break
        if not refs:
            return {"type": "native"}
        return {**definition, "strategies": refs,
                "evidence": {"source_column": key + "_source", "column": key + "_evidence"}}

    out = {}
    for name, meta in tables_meta.items():
        if name.startswith("_") or not isinstance(meta, dict):
            continue
        if meta.get("export") is False:
            continue
        if "catalog" not in meta:
            continue
        cat = build_table_catalog(name, meta)
        for field in cat["fields"]:
            field["provenance"] = field_provenance(name, field["key"])
        entity = meta.get("entity", name)
        def native(key):
            if key in meta.get("system_columns", ["source_platform"]) or "_e_" in key:
                return key
            return f"{entity}_e_{'id' if key == meta.get('grain_pk') else meta.get('source', {}).get('carried_keys', {}).get(key, key)}"
        identities = list(dict.fromkeys([native(k) for k in [meta.get("grain_pk"), *meta.get("own_keys", [])] if k]
                         + meta.get("system_columns", ["source_platform"])))
        actual = set(schema_columns[name]) if schema_columns and name in schema_columns else None
        fields = [f["key"] for f in cat["fields"]]
        # Identity columns can be queried for drilldown even when hidden from editors.
        allowed = list(dict.fromkeys(fields + identities))
        if actual is not None:
            allowed = [k for k in allowed if k in actual]
            identities = [k for k in identities if k in actual]
            for field in cat["fields"]:
                evidence = field["provenance"].get("evidence")
                if evidence and not {evidence["column"], evidence["source_column"]} <= actual:
                    raise ValueError(f"Missing generated evidence for {name}.{field['key']}")
        cat["record_identity"] = identities
        cat["record_grain"] = meta.get("source", {}).get("grain", name)
        cat["evidence_query_fields"] = allowed
        cat["record_fields"] = allowed
        dedup = meta.get("dedup")
        if dedup:
            cat["row_provenance"] = {"strategies": [reference("dedup", "none" if dedup["strategies"][0]["strategy"] == "none" else "pipeline")],
                "evidence": {"source_column": "__mm_dedup_source", "column": "__mm_dedup_evidence"},
                "policy": copy.deepcopy(dedup), "identity_links": copy.deepcopy(meta.get("identity_links", [])),
                "audit_path": f"audit/dedup/{run_id}/{name}" if dedup["strategies"][0]["strategy"] != "none" else None}
        out[name] = cat
    # Include all strategies actually configured, including non-exported stages,
    # once per client. Unused registered strategies are not published.
    for tm in tables_meta.values():
        if not isinstance(tm, dict):
            continue
        for spec in tm.get("resolve", {}).values():
            for steps in resolver_variants(spec).values():
                for step in steps:
                    reference("resolve", step["strategy"])
        for stage, section, op in [("metric", "metrics", "agg"), ("derive", "derive", "expr")]:
            for spec in tm.get(section, []):
                reference(stage, spec[op])
        for spec in tm.get("enrich", []):
            reference("enrich", spec.get("strategy", "left_join_bring"))
        if tm.get("dedup"):
            for step in tm["dedup"]["strategies"]:
                reference("dedup", step["strategy"])
    return {"schema_version": 2, "run_id": run_id, "tables": out,
            "strategies": {k: copy.deepcopy(strategies[k]) for k in sorted(used)}}


def write_catalog(tables_meta: dict, out_path: str, **kwargs):
    """Build the full catalog and write it as JSON to out_path (e.g. an ADLS
    path in the gold container). Called by gold_build after tables are written."""
    catalog = build_full_catalog(tables_meta, **kwargs)
    payload = json.dumps(catalog, indent=2)
    # dbutils is available in Databricks; fall back to open() elsewhere (tests).
    try:
        dbutils.fs.put(out_path, payload, overwrite=True)  # noqa: F821
    except NameError:
        with open(out_path, "w") as f:
            f.write(payload)
    print(f"✓ catalog.json written ({len(catalog['tables'])} tables) -> {out_path}")
    return catalog
