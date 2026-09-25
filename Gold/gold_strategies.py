# Databricks notebook source
# Gold strategies -- registered into the four-stage engine
# ─────────────────────────────────────────────────────────────────────────────
# Ports the existing resolver logic into the stage registries, and adds the
# metric strategies that reproduce the current fact window block. Every function
# is registered via a decorator from gold_engine; the engine dispatches by name
# from the table metadata.

# COMMAND ----------

# MAGIC %run ./gold_engine

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, lit, when, lower, trim, regexp_replace, coalesce,
    sum as spark_sum, min as spark_min, max as spark_max, count as spark_count,
    countDistinct, first, date_trunc, datediff, current_date, size, collect_set,
    date_format, row_number, struct, to_json, dense_rank,
)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS (unchanged from the current resolvers)
# ══════════════════════════════════════════════════════════════════════════════

def company_match_key(c):
    k = lower(trim(c))
    k = regexp_replace(k, r"[.,]", "")
    k = regexp_replace(k, r"\b(llc|inc|incorporated|corp|co|ltd)\b", "")
    k = regexp_replace(k, r"\s+", " ")
    return trim(k)


# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1 -- DEDUP strategies
# ══════════════════════════════════════════════════════════════════════════════

@dedup_strategy("none",
    description="Keeps all incoming rows; no deduplication is applied.",
    outcomes={"retained":"This row was retained without deduplication."})
def dedup_none(df, spec, ctx):
    """No dedup -- table is already at its own grain."""
    return df


@dedup_strategy("keep_first",
    description="Keeps one row per identity, ordered descending by the configured ordering field. Ties have no explicit preference.",
    outcomes={"retained":"This is the retained row. Discarded candidates are not stored in this evidence."})
def dedup_keep_first(df, spec, ctx):
    """One row per `on` key, deterministic pick by optional `order_by` (desc)."""
    on = spec["on"]
    order_col = spec.get("order_by", on)
    w = Window.partitionBy(on).orderBy(col(order_col).desc())
    return (df.withColumn("_rn", row_number().over(w))
              .filter(col("_rn") == 1).drop("_rn"))

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2 -- RESOLVE strategies (ported from the current resolvers)
# ══════════════════════════════════════════════════════════════════════════════

def _candidate(df, value, reason, inputs, ambiguous=None):
    usable = value.isNotNull() & (trim(value.cast("string")) != "")
    status = when(usable, lit("resolved")).otherwise(lit("no_match"))
    if ambiguous is not None:
        status = when(ambiguous, lit("ambiguous")).otherwise(status)
    return df.withColumn("__mm_candidate", struct(
        when(usable, value.cast("string")).alias("value"),
        status.alias("status"),
        when(status == "ambiguous", lit("ambiguous_lookup"))
            .when(usable, lit(reason)).otherwise(lit("missing_input_or_match")).alias("reason"),
        to_json(struct(*[v.alias(k) for k, v in inputs.items()]),
                {"ignoreNullFields": "false"}).alias("inputs")))


def _unique_lookup(df, key, fields):
    """One lookup row per key. Conflicting payloads are flagged, never picked."""
    values = struct(*[col(field).alias(field) for field in fields])
    return (df.filter(col(key).isNotNull()).groupBy(key)
        .agg(collect_set(values).alias("__mm_matches"))
        .withColumn("__mm_ambiguous", size(col("__mm_matches")) > 1)
        .select(col(key).alias("__mm_lookup_key"), "__mm_ambiguous",
            *[when(~col("__mm_ambiguous"), col("__mm_matches")[0][field])
                .alias("__mm_lookup_" + field) for field in fields]))


@resolve_strategy("company_order_string",
    row_inputs={"field": "orders_e_customer_company"},
    description="Uses normalized company text from the configured row field as the company identity. No directory lookup is attempted.",
    outcomes={"order_string": "Normalized order company text supplied the identity.",
              "missing_input_or_match": "No usable company text was supplied."})
def r_company_order_string(df, params, ctx):
    value = company_match_key(col(params["field"]))
    return _candidate(df, value, "order_string",
        {"order_company": col(params["field"]), "normalized_name": value})


@resolve_strategy("company_normalized_name_match",
    row_inputs={"field": "orders_e_customer_company"},
    description="Matches normalized row company text to the companies directory. Normalization removes case, punctuation, selected suffixes and repeated spaces. Conflicting matches stop the build.",
    outcomes={"company_name_match": "A unique normalized-name match supplied the company identity.",
              "missing_input_or_match": "No usable company name or directory match was found."})
def r_company_normalized_name_match(df, params, ctx):
    lookup = _unique_lookup(ctx["dims"]["companies"].withColumn(
        "__mm_name", company_match_key(col("company_e_name"))),
        "__mm_name", ["company_e_id", "company_e_name", "source_platform"])
    key = company_match_key(col(params["field"]))
    joined = df.join(lookup, (key != "") & (key == col("__mm_lookup_key")), "left")
    result = _candidate(joined, col("__mm_lookup_company_e_id"), "company_name_match", {
        "order_company": col(params["field"]), "normalized_name": key,
        "matched_company_id": col("__mm_lookup_company_e_id"),
        "matched_company_name": col("__mm_lookup_company_e_name"),
        "matched_platform": col("__mm_lookup_source_platform")},
        col("__mm_ambiguous"))
    return result.drop(*lookup.columns)


@resolve_strategy("sales_rep_order_native", version=2,
    row_inputs={"field": "orders_e_sales_rep_email"},
    description="Uses the rep email recorded in the configured row field. Missing or blank values leave the row unresolved for the next configured step.",
    outcomes={"order_native": "The order's recorded sales rep supplied the email.",
              "missing_input_or_match": "The order had no usable sales rep email."})
def r_rep_order_native(df, params, ctx):
    return _candidate(df, col(params["field"]), "order_native",
        {"order_rep": col(params["field"])})


@resolve_strategy("sales_rep_company_owner",
    row_inputs={"company_field": "company_e_id"},
    description="Looks up the resolved company in the companies directory, then looks up its owner in the rep directory. It does not read the order's native rep.",
    outcomes={"company_owner": "The company's owner supplied the sales rep email.",
              "missing_input_or_match": "The company, owner ID or owner email could not be matched."})
def r_rep_company_owner(df, params, ctx):
    companies = _unique_lookup(ctx["dims"]["companies"], "company_e_id", ["company_e_owner_id"])
    joined = df.join(companies, col(params["company_field"]) == col("__mm_lookup_key"), "left")
    joined = (joined.withColumnRenamed("__mm_lookup_key", "__mm_company_key")
        .withColumnRenamed("__mm_ambiguous", "__mm_company_ambiguous"))
    reps = _unique_lookup(ctx["dims"]["sales_reps"], "sales_rep_e_id", ["sales_rep_e_email"])
    joined = joined.join(reps, col("__mm_lookup_company_e_owner_id") == col("__mm_lookup_key"), "left")
    inputs = {"company_id": col(params["company_field"]), "matched_company_id": col("__mm_company_key"),
        "owner_id": col("__mm_lookup_company_e_owner_id"), "matched_rep_id": col("__mm_lookup_key"),
        "owner_email": col("__mm_lookup_sales_rep_e_email")}
    upstream = params["company_field"] + "_evidence"
    if upstream in df.columns:
        inputs["company_decision"] = col(upstream)
    result = _candidate(joined, col("__mm_lookup_sales_rep_e_email"), "company_owner", inputs,
        coalesce(col("__mm_company_ambiguous"), lit(False)) | coalesce(col("__mm_ambiguous"), lit(False)))
    return result.drop("__mm_company_key", "__mm_company_ambiguous",
        "__mm_lookup_company_e_owner_id", *reps.columns)


@resolve_strategy("owner_id_to_rep_email", version=2,
    row_inputs={"owner_field": "company_e_owner_id"},
    description="Looks up the configured row owner ID in the rep directory and returns its unique email.",
    outcomes={"owner_lookup": "An email was returned by the owner-ID lookup.",
              "missing_input_or_match": "No usable owner ID or matching rep email was found."})
def r_owner_id_to_rep_email(df, params, ctx):
    reps = _unique_lookup(ctx["dims"]["sales_reps"], "sales_rep_e_id", ["sales_rep_e_email"])
    joined = df.join(reps, col(params["owner_field"]) == col("__mm_lookup_key"), "left")
    result = _candidate(joined, col("__mm_lookup_sales_rep_e_email"), "owner_lookup",
        {"owner_id": col(params["owner_field"]), "matched_rep_id": col("__mm_lookup_key"),
         "matched_email": col("__mm_lookup_sales_rep_e_email")}, col("__mm_ambiguous"))
    return result.drop(*reps.columns)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 3 -- METRIC strategies
# Declarative aggregates + windowed metrics. Each returns ONE Column, partitioned
# over the metric's `over` (an identity column). Reproduces the current window block.
# ══════════════════════════════════════════════════════════════════════════════

def _win(m):
    return Window.partitionBy(m["over"])

def _win_ordered(m):
    return (Window.partitionBy(m["over"])
            .orderBy(col(m.get("order_by", "order_date")).asc())
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

@metric_strategy("sum",
    description="Adds the input values within the configured identity partition.",
    outcomes={})
def m_sum(df, m, ctx):
    return spark_sum(m["of"]).over(_win(m))

@metric_strategy("min",
    description="Finds the lowest input value within the configured identity partition.",
    outcomes={})
def m_min(df, m, ctx):
    return spark_min(m["of"]).over(_win(m))

@metric_strategy("max",
    description="Finds the highest input value within the configured identity partition.",
    outcomes={})
def m_max(df, m, ctx):
    return spark_max(m["of"]).over(_win(m))

@metric_strategy("count_distinct",
    description="Counts distinct non-null input values within the configured identity partition.",
    outcomes={})
def m_count_distinct(df, m, ctx):
    # size(collect_set(...)) over a window -- distinct count without collapsing rows
    return size(collect_set(m["of"]).over(_win(m)))

@metric_strategy("first_ordered",
    description="Takes the input value from the earliest ordered row in the identity partition. Ordering ties have no explicit preference.",
    outcomes={})
def m_first_ordered(df, m, ctx):
    return first(m["of"]).over(_win_ordered(m))

@metric_strategy("cohort_month",
    description="Formats the earliest input date in the identity partition as year-month.",
    outcomes={})
def m_cohort_month(df, m, ctx):
    # yyyy-MM of the earliest order_date in the partition
    return date_format(spark_min(m["of"]).over(_win(m)), "yyyy-MM")

@metric_strategy("days_since",
    description="Calculates days from the latest input date in the identity partition to the pipeline processing date, not the viewing date.",
    outcomes={})
def m_days_since(df, m, ctx):
    return datediff(current_date(), spark_max(m["of"]).over(_win(m)))

@metric_strategy("is_current_month_of",
    description="Checks whether the configured row date, truncated to month, equals the configured comparison value.",
    outcomes={})
def m_is_current_month(df, m, ctx):
    # true if this row's order_date month == the cohort month column named in `of`
    return date_trunc("month", col(m["order_date"])) == col(m["of"])

@metric_strategy(
    "sequence",
    description=(
        "Assigns a consecutive sequence within the configured identity "
        "partition, ordered by the configured field and direction. "
        "The entity key breaks ordering ties. Repeated rows with the same "
        "ordering value and entity key share a sequence number. "
        "Rows missing a required input receive null and do not affect "
        "the sequence of valid rows."
    ),
    outcomes={}
)
def m_sequence(df, m, ctx):
    """
    Required:
      over:     Partition identity, e.g. customer ID.
      of:       Entity identity, e.g. globally unique order ID.
      order_by: Ordering field, e.g. order timestamp.

    Optional:
      direction: "asc" (default) or "desc".

    All rows for an entity must share the same partition identity
    and ordering value.
    """
    partition_key = m["over"]
    entity_key = m["of"]
    ordering_key = m["order_by"]
    direction = m.get("direction", "asc").lower()

    if direction not in ("asc", "desc"):
        raise ValueError("sequence.direction must be 'asc' or 'desc'")

    for key in (partition_key, entity_key, ordering_key):
        if not isinstance(key, str) or key not in df.columns:
            raise ValueError(
                f"sequence requires an existing column name; received {key!r}"
            )

    valid = (
        col(partition_key).isNotNull()
        & col(entity_key).isNotNull()
        & col(ordering_key).isNotNull()
    )

    ordering = (
        col(ordering_key).asc()
        if direction == "asc"
        else col(ordering_key).desc()
    )

    # Separate invalid rows so they cannot shift valid sequence numbers.
    window = (
        Window.partitionBy(col(partition_key), valid)
        .orderBy(ordering, col(entity_key).asc())
    )

    return when(
        valid,
        dense_rank().over(window)
    ).otherwise(lit(None).cast("int"))

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4 -- ENRICH strategies
# ══════════════════════════════════════════════════════════════════════════════

@enrich_strategy("left_join_bring",
    description="Brings fields from a source table's public surface by key using a left join. Source rows are reduced to one per join key without an explicit tie preference.",
    outcomes={"matched":"A source row matched the join key. The brought value itself may be null.","unmatched":"No source row matched the join key."})
def e_left_join_bring(df, source_surface, spec, ctx):
    """Left-join a source table's public surface on `on`, bring renamed columns.
    `bring` is {source_col: target_col}. Collisions avoided by explicit renaming."""
    return _enrich_with_evidence(df, source_surface, spec, ctx, "left", e_left_join_bring.strategy_ref)
    
@enrich_strategy("full_join_bring",
    description="Brings fields by key using a full outer join, preserving unmatched rows from either side. Source rows are reduced to one per join key without an explicit tie preference.",
    outcomes={"matched":"A source row was present for this enrichment.","unmatched":"No source row was present for this enrichment."})
def e_full_join_bring(df, source_surface, spec, ctx):
    """Full-join a source table's public surface on `on`, 
    bring renamed columns."""
    return _enrich_with_evidence(df, source_surface, spec, ctx, "full_outer", e_full_join_bring.strategy_ref)


def _enrich_with_evidence(df, source_surface, spec, ctx, how, ref):
    on, bring = spec["on"], spec["bring"]
    src_on = spec.get("source_on", on)
    join_key = "__mm_enrich_key"
    selects = [col(src_on).alias(join_key)]
    scratch = [join_key]
    for index, (source, target) in enumerate(bring.items()):
        selects.append(col(source).alias(target))
        upstream = f"__mm_upstream_{index}"
        scratch.append(upstream)
        selects.append((col(source + "_evidence") if source + "_evidence" in source_surface.columns
                        else lit(None).cast("string")).alias(upstream))
    src = source_surface.select(*selects).dropDuplicates([join_key])
    result = df.join(src, df[on] == src[join_key], how)
    for index, (source, target) in enumerate(bring.items()):
        result = record_evidence(result, target, ref,
            when(col(join_key).isNotNull(), lit("matched")).otherwise(lit("unmatched")),
            {"join_value": col(on), "matched_key": col(join_key),
             "source_table": lit(spec["from"]), "source_field": lit(source),
             "upstream_decision": col(f"__mm_upstream_{index}")}, ctx)
    return result.drop(*scratch)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4 tail -- DERIVE strategies (post-enrich per-row expressions)
# May reference any column on the row (native/resolved/metric/enriched). Terminal.
# ══════════════════════════════════════════════════════════════════════════════

@derive_strategy("coalesce",
    description="Uses the first non-null value from the configured inputs, in their declared order.",
    outcomes={})
def d_coalesce(df, d, ctx):
    """First non-null across `of` (a list of column names). Display-name fallback:
    coalesce(hubspot_name, order_string_name)."""
    return coalesce(*[col(c) for c in d["of"]])


@derive_strategy("is_not_null",
    description="Returns true when the configured input is non-null; false otherwise.",
    outcomes={})
def d_is_not_null(df, d, ctx):
    """Boolean flag: true when `of` (a column) is non-null. The in_hubspot flag."""
    return col(d["of"]).isNotNull()

# COMMAND ----------

print("Gold strategies loaded and registered.")
print(f"  dedup:   {sorted(DEDUP)}")
print(f"  resolve: {sorted(RESOLVE)}")
print(f"  metric:  {sorted(METRIC)}")
print(f"  enrich:  {sorted(ENRICH)}")
print(f"  derive:  {sorted(DERIVE)}")
