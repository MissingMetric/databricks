# Databricks notebook source
# MAGIC %run ./gold_dedup_config
# COMMAND ----------
import json
from functools import reduce
from pyspark.sql import functions as F, Window


def _dd_json(columns):
    return F.to_json(F.struct(*columns), {"ignoreNullFields": "false"})


def _dd_identity(fields, values=None):
    return _dd_json([(F.lit(values[k]) if values else F.col(k)).cast("string").alias(k) for k in fields])


def _dd_order(items):
    return [F.col(i["field"]).desc_nulls_last() if i["direction"] == "desc"
            else F.col(i["field"]).asc_nulls_last() for i in items]


def _dd_normalize(key):
    value = F.col(key["field"])
    mode = key.get("normalize", "exact")
    if mode != "exact":
        value = F.lower(F.trim(value.cast("string")))
    if mode == "company_name":
        value = F.regexp_replace(value, r"[.,]", "")
        value = F.regexp_replace(value, r"\b(llc|inc|incorporated|corp|co|ltd)\b", "")
        value = F.trim(F.regexp_replace(value, r"\s+", " "))
    return value


def _dd_explicit_links(nodes, spec, step):
    name, params = step["strategy"], step.get("params", {})
    parts = []
    for pair in params["aliases"]:
        left = nodes.filter(F.col("__dd_identity") == _dd_identity(spec["identity"], pair["from"]))
        right = nodes.filter(F.col("__dd_identity") == _dd_identity(spec["identity"], pair["to"]))
        if not left.limit(1).count() or not right.limit(1).count():
            raise ValueError(f"{step.get('id', name)}: alias endpoint missing; no record was removed")
        parts.append(left.select(F.col("__dd_identity").alias("a")).crossJoin(
            right.select(F.col("__dd_identity").alias("b"))).withColumn("match_inputs", F.lit(json.dumps(pair))))
    edges = reduce(lambda a, b: a.unionByName(b), parts)
    return edges

def _dd_external_links(nodes, spec, step):
    name, params = step["strategy"], step.get("params", {})
    def scoped(bindings):
        return nodes.filter(reduce(lambda a, b: a & b, [F.col(k).cast("string") == F.lit(v) for k, v in bindings.items()]))
    left = scoped(params["from_scope"]).select(F.col("__dd_identity").alias("a"), F.col(params["reference_field"]).cast("string").alias("ref"))
    right = scoped(params["to_scope"]).select(F.col("__dd_identity").alias("b"), F.col(params["target_field"]).cast("string").alias("ref"))
    joined = left.filter(F.col("ref").isNotNull() & (F.trim("ref") != "")).join(right, "ref").filter(F.col("a") != F.col("b"))
    if step.get("action", "merge") == "merge":
        for side in ("a", "b"):
            if joined.groupBy(side).count().filter("count > 1").limit(1).count():
                raise ValueError(f"{step.get('id', name)}: external references are not one-to-one; no automatic consolidation")
    edges = joined.select("a", "b", _dd_json([F.col("ref").alias("reference"),
        F.lit(json.dumps(params["from_scope"])).alias("from_scope"),
        F.lit(json.dumps(params["to_scope"])).alias("to_scope")]).alias("match_inputs"))
    return edges

def _dd_key_links(nodes, spec, step):
    name, params = step["strategy"], step.get("params", {})
    keys = [_dd_normalize(k) for k in params["keys"]]
    valid = reduce(lambda a, b: a & b, [v.isNotNull() & (F.trim(v.cast("string")) != "") for v in keys])
    keyed = nodes.filter(valid).withColumn("__dd_key", _dd_json(
        [F.col(k).cast("string").alias("scope_" + k) for k in spec["scope"]] +
        [v.alias("key_" + str(i)) for i, v in enumerate(keys)]))
    groups = keyed.groupBy("__dd_key").agg(F.min("__dd_identity").alias("b"), F.count("*").alias("n")).filter("n > 1")
    edges = keyed.join(groups, "__dd_key").select(F.col("__dd_identity").alias("a"), "b", F.col("__dd_key").alias("match_inputs"))
    return edges

DEDUP_MATCHERS = {
    "unique_business_key": _dd_key_links,
    "composite_fingerprint": _dd_key_links,
    "explicit_identity_mapping": _dd_explicit_links,
    "external_reference": _dd_external_links,
}


def _dd_links(nodes, spec, step):
    """Matchers propose evidence-bearing edges; only the engine selects records."""
    name = step["strategy"]
    edges = DEDUP_MATCHERS[name](nodes, spec, step)
    return (edges.filter(F.col("a") != F.col("b"))
        .withColumn("step", F.lit(step.get("id", name)))
        .withColumn("strategy", F.lit(f"dedup.{name}@1"))
        .withColumn("action", F.lit(step.get("action", "flag" if name == "composite_fingerprint" else "merge"))))


def _dd_components(nodes, edges):
    """Bounded distributed connected components; weaker flag-only edges never enter."""
    labels = nodes.select(F.col("__dd_identity").alias("id")).withColumn("component", F.col("id")).localCheckpoint(eager=True)
    adjacency = edges.select("a", "b").unionByName(edges.select(F.col("b").alias("a"), F.col("a").alias("b"))).distinct().localCheckpoint(eager=True)
    try:
        for _ in range(64):
            propagated = adjacency.join(labels, adjacency.a == labels.id).select(F.col("b").alias("id"), "component")
            updated = labels.unionByName(propagated).groupBy("id").agg(F.min("component").alias("component")).localCheckpoint(eager=True)
            changed = updated.alias("n").join(labels.alias("p"), "id").filter(F.col("n.component") != F.col("p.component")).limit(1).count()
            labels.unpersist()
            labels = updated
            if not changed:
                return labels
        raise ValueError("Dedup graph exceeded 64 iterations; no partial groups were accepted")
    finally:
        adjacency.unpersist()


def run_dedup_pipeline(df, table_meta, ctx):
    spec = validate_dedup(table_meta.get("dedup"), df.columns)
    name = table_meta.get("_name", table_meta.get("entity", "table"))
    reserved = {"component", "canonical_identity", "selected_record", "source_identity", "conflict", "occurrences"}
    if any(k.startswith("__dd_") or k.startswith("__mm_dedup") or k in reserved for k in df.columns):
        raise ValueError("Input contains reserved dedup columns")
    native = df.columns
    steps = spec["strategies"]
    if steps[0]["strategy"] == "none":
        return (df.withColumn("__mm_dedup_source", F.lit("retained"))
            .withColumn("__mm_dedup_evidence", _dd_json([F.lit("dedup.none@1").alias("strategy"),
                F.lit("retained").alias("source"), F.lit(ctx["run_id"]).alias("run_id")])) )
    missing = reduce(lambda a, b: a | b, [F.col(k).isNull() | (F.trim(F.col(k).cast("string")) == "") for k in spec["identity"]])
    if df.filter(missing).limit(1).count():
        raise ValueError(f"{name}: missing source identity/account; cannot deduplicate safely")
    encoded = df.withColumn("__dd_payload", _dd_json([F.col(k).alias(k) for k in sorted(native)]))
    # Full payload equality, not hash equality, defines exact duplicate records.
    occurrences = encoded.groupBy("__dd_payload").agg(F.count("*").alias("occurrences"))
    all_rows = (encoded.dropDuplicates(["__dd_payload"]).join(occurrences, "__dd_payload")
        .withColumn("__dd_identity", _dd_identity(spec["identity"]))).localCheckpoint(eager=True)
    nodes = all_rows
    if len(steps) > 1 and steps[1]["strategy"] == "same_source_identity":
        order = _dd_order(steps[1]["params"]["order_by"])
        latest = nodes.withColumn("__dd_rank", F.dense_rank().over(Window.partitionBy("__dd_identity").orderBy(*order))).filter("__dd_rank = 1").drop("__dd_rank")
        if latest.groupBy("__dd_identity").count().filter("count > 1").limit(1).count():
            raise ValueError(f"{name}: conflicting source versions have equal revision rank; add a trustworthy revision field")
        nodes = latest
    elif nodes.groupBy("__dd_identity").count().filter("count > 1").limit(1).count():
        raise ValueError(f"{name}: multiple versions require same_source_identity")
    nodes = nodes.localCheckpoint(eager=True)
    empty_edges = nodes.select(*[F.lit(None).cast("string").alias(k)
        for k in ("a", "b", "match_inputs", "step", "strategy", "action")]).limit(0)
    edges = empty_edges
    for step in steps:
        if step["strategy"] in ("exact_record", "same_source_identity"):
            continue
        edges = edges.unionByName(_dd_links(nodes, spec, step))
    edges = edges.localCheckpoint(eager=True)
    labels = _dd_components(nodes, edges.filter("action = 'merge'"))
    grouped = nodes.join(labels.select(F.col("id").alias("__dd_identity"), "component"), "__dd_identity")
    conflicts = grouped.select("component").limit(0)
    for key in spec.get("conflict_fields", []):
        bad = grouped.groupBy("component").agg(F.countDistinct(_dd_json([F.col(key).alias("value")])).alias("n")).filter("n > 1").select("component")
        conflicts = conflicts.unionByName(bad)
    conflicts = conflicts.distinct().localCheckpoint(eager=True)
    conflict_records = grouped.join(conflicts, "component").select("component", F.col("__dd_identity").alias("source_identity"), F.col("__dd_payload").alias("record"))
    ctx.setdefault("dedup_conflicts", {})[name] = conflict_records
    if spec.get("on_conflict", "fail") == "fail" and conflicts.limit(1).count():
        raise ValueError(f"{name}: conflicting protected fields; inspect ctx['dedup_conflicts'][{name!r}]. No consolidation accepted")
    # Retain every entity in conflicted components; do not pick a representative.
    safe = grouped.join(conflicts.withColumn("__dd_conflict", F.lit(True)), "component", "left")
    safe = safe.withColumn("__dd_group", F.when(F.col("__dd_conflict"), F.col("__dd_identity")).otherwise(F.col("component")))
    requested = (edges.filter("strategy = 'dedup.explicit_identity_mapping@1' AND action = 'merge'")
        .select(F.col("b").alias("id")).distinct().join(labels, "id")
        .groupBy("component").agg(F.collect_set("id").alias("__dd_requested")))
    if requested.filter(F.size("__dd_requested") > 1).limit(1).count():
        raise ValueError("Explicit aliases disagree on a canonical identity; use direct aliases to one canonical record")
    safe = safe.join(requested, "component", "left")
    preferences = []
    for preference in spec.get("selection", {}).get("prefer_values", []):
        rank = F.lit(len(preference["values"]))
        for index, value in reversed(list(enumerate(preference["values"]))):
            rank = F.when(F.col(preference["field"]).cast("string") == value, F.lit(index)).otherwise(rank)
        preferences.append(rank.asc())
    ordering = [(F.col("__dd_identity") == F.col("__dd_requested")[0]).desc_nulls_last()] + preferences + _dd_order(spec.get("selection", {}).get("order_by", [])) + [F.col("__dd_identity").asc()]
    winners = safe.withColumn("__dd_pick", F.row_number().over(Window.partitionBy("__dd_group").orderBy(*ordering))).filter("__dd_pick = 1")
    choices = winners.select("__dd_group", F.col("__dd_identity").alias("canonical_identity"), F.col("__dd_payload").alias("selected_record"))
    aliases = safe.select("__dd_group", "__dd_identity", "__dd_conflict").join(choices, "__dd_group").select(
        F.col("__dd_identity").alias("source_identity"), "canonical_identity", "selected_record",
        F.coalesce("__dd_conflict", F.lit(False)).alias("conflict"))
    aliases = aliases.localCheckpoint(eager=True)
    ctx.setdefault("dedup_aliases", {})[name] = aliases
    evidence = _dd_json([F.lit("dedup.pipeline@1").alias("strategy"), F.lit("retained").alias("source"),
        F.lit(ctx["run_id"]).alias("run_id"), F.lit(ctx["processed_at"]).alias("processed_at"),
        F.col("__dd_identity").alias("canonical_identity"), F.lit(json.dumps(spec)).alias("policy"),
        F.coalesce("__dd_conflict", F.lit(False)).alias("conflict"), F.lit(f"audit/dedup/{ctx['run_id']}/{name}").alias("audit_path")])
    result = winners.withColumn("__mm_dedup_source", F.lit("retained")).withColumn("__mm_dedup_evidence", evidence).select(*native, "__mm_dedup_source", "__mm_dedup_evidence").localCheckpoint(eager=True)
    records = all_rows.join(aliases, all_rows.__dd_identity == aliases.source_identity).select(
        "source_identity", "canonical_identity", "occurrences", "conflict",
        F.col("__dd_payload").alias("source_record"), "selected_record",
        F.when(F.col("__dd_payload") == F.col("selected_record"), F.lit("selected"))
            .when(F.col("source_identity") == F.col("canonical_identity"), F.lit("superseded_version"))
            .otherwise(F.lit("consolidated_alias")).alias("decision"))
    ctx.setdefault("dedup_audits", {})[name] = {"records": records.localCheckpoint(eager=True),
        "matches": edges, "conflicts": conflict_records.localCheckpoint(eager=True)}
    ctx["dedup_conflicts"][name] = ctx["dedup_audits"][name]["conflicts"]
    for frame in (all_rows, nodes, labels, conflicts):
        frame.unpersist()
    return result


def apply_identity_links(df, table_meta, tables_meta, ctx):
    """Remap declared foreign IDs before native prefixing and resolver execution."""
    for index, link in enumerate(table_meta.get("identity_links", [])):
        target = tables_meta[link["table"]]["dedup"]["identity"]
        mapping = ctx.get("dedup_aliases", {}).get(link["table"])
        if mapping is None:
            raise ValueError(f"No identity mapping published for {link['table']}")
        bindings = {**link["scope"], link["key"]: link["field"]}
        key = _dd_json([F.col(bindings[k]).cast("string").alias(k) for k in target])
        aliases = mapping.select(F.col("source_identity").alias("__dd_link_from"), F.col("canonical_identity").alias("__dd_link_to"))
        joined = df.join(aliases, key == F.col("__dd_link_from"), "left")
        value = F.from_json(F.col("__dd_link_to"), "map<string,string>")[link["key"]]
        trace = _dd_json([F.col(link["field"]).alias("original_value"), value.alias("canonical_value"),
            F.lit(link["table"]).alias("lookup_table"), F.lit(ctx["run_id"]).alias("run_id")])
        df = (joined.withColumn(f"__mm_identity_link_{index}", trace)
            .withColumn(link["field"], F.coalesce(value.cast(df.schema[link["field"]].dataType), F.col(link["field"])))
            .drop("__dd_link_from", "__dd_link_to"))
    return df
