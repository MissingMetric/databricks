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
    date_format, row_number,
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


def company_owner_email_lookup(dims: dict) -> DataFrame:
    """company.owner_id -> sales_reps.rep_email. Same helper the dimension uses."""
    companies = dims["companies"]
    reps = dims["sales_reps"]
    rep_lookup = reps.select(
        col("sales_rep_e_id").alias("_rep_id"),
        col("sales_rep_e_email").alias("_rep_email"),
    ).dropDuplicates(["_rep_id"])
    return (
        companies
        .select(col("company_e_id").alias("_oc_company_id"), 
        col("company_e_owner_id").alias("_oc_owner_id"))
        .join(rep_lookup, col("_oc_owner_id") == col("_rep_id"), "left")
        .select(col("_oc_company_id"), col("_oc_owner_id"), col("_rep_email").alias("_company_owner_email"))
        .dropDuplicates(["_oc_company_id"])
    )

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

@resolve_strategy("company_order_string_only",
    description="Uses normalized company text from the order as the company identity; no company lookup is performed.",
    outcomes={"order_string_only":"The order-text-only strategy ran. A missing name may still yield an unresolved identity."})
def r_company_order_string_only(df, spec, ctx):
    result = (
        df
        .withColumn("company_key", company_match_key(col("orders_e_customer_company")))
        .withColumn("company_e_id",
            when(col("company_key") == "", lit("unresolved"))
            .when(col("company_key").isNull(), lit("unresolved"))
            .otherwise(col("company_key")))
        .withColumn("company_e_source", lit("order_string_only"))
        .withColumn("company_e_mismatch", lit(False))
    )
    return record_evidence(result, spec["_target"], r_company_order_string_only.strategy_ref,
        col("company_e_source"), {"order_company": col("orders_e_customer_company"),
        "normalized_name": col("company_key")}, ctx, result_column="company_e_id").drop("company_key")


@resolve_strategy("company_hubspot_match_with_fallback",
    description="Matches normalized order company text against the companies dimension, falling back to normalized order text. Matching removes case, periods, commas, selected company suffixes, and repeated spaces.",
    outcomes={"hubspot":"A normalized-name match was found in the companies dimension. The historical 'hubspot' tag alone does not establish the matched record's platform.","order_string":"No lookup match was found; normalized order company text supplied the identity.","unresolved":"Neither a lookup match nor usable order company text supplied an identity."})
def r_company_hubspot_match(df, spec, ctx):
    companies = ctx["dims"]["companies"]
    comp_lookup = (
        companies
        .withColumn("_ckey", company_match_key(col("company_e_name")))
        .select(col("company_e_id").alias("_hs_company_id"), col("_ckey"),
                col("company_e_name").alias("_matched_name"),
                col("source_platform").alias("_matched_platform"))
        .dropDuplicates(["_ckey"])
    )
    f = df.withColumn("company_key", company_match_key(col("orders_e_customer_company")))
    joined = f.join(comp_lookup, f["company_key"] == comp_lookup["_ckey"], "left")
    result = (
        joined
        .withColumn("company_e_id",
            coalesce(col("_hs_company_id"),
                     when(col("company_key") != "", col("company_key"))))
        .withColumn("company_e_id", coalesce(col("company_e_id"), lit("unresolved")))
        .withColumn("company_e_source",
            when(col("_hs_company_id").isNotNull(), lit("hubspot"))
            .when(col("company_key") != "", lit("order_string"))
            .otherwise(lit("unresolved")))
        .withColumn("company_e_mismatch",
            (col("company_key") != "") & col("_hs_company_id").isNull())
    )
    return record_evidence(result, spec["_target"], r_company_hubspot_match.strategy_ref,
        col("company_e_source"), {"order_company": col("orders_e_customer_company"),
        "normalized_name": col("company_key"), "matched_company_id": col("_hs_company_id"),
        "matched_company_name": col("_matched_name"), "matched_platform": col("_matched_platform")},
        ctx, result_column="company_e_id").drop("company_key", "_ckey", "_hs_company_id", "_matched_name", "_matched_platform")


@resolve_strategy("sales_rep_order_native",
    description="Uses the sales rep recorded on the order. This strategy does not attempt a company-owner fallback.",
    outcomes={"order_native":"The order rep was non-null and was used directly.","unresolved":"The order rep was null; this strategy has no fallback."})
def r_rep_order_native(df, spec, ctx):
    result = (
        df
        .withColumn("sales_rep_e_email", col("orders_e_sales_rep_email"))
        .withColumn("sales_rep_e_source",
            when(col("orders_e_sales_rep_email").isNotNull(), lit("order_native"))
            .otherwise(lit("unresolved")))
    )
    return record_evidence(result, spec["_target"], r_rep_order_native.strategy_ref,
        col("sales_rep_e_source"), {"order_rep": col("orders_e_sales_rep_email")}, ctx, result_column="sales_rep_e_email")


@resolve_strategy("sales_rep_native_then_company_owner",
    description="Uses the order rep when non-null; otherwise joins the resolved company to its owner ID and looks up that owner's rep email.",
    outcomes={"order_native":"The order rep was non-null and won over the owner fallback.","company_owner":"The order rep was null. The matched company's owner lookup supplied the rep email.","unresolved":"The order rep was null and the owner lookup supplied no email. Inspect the recorded inputs for missing values."})
def r_rep_native_then_company_owner(df, spec, ctx):
    owner_lookup = company_owner_email_lookup(ctx["dims"])
    joined = df.join(owner_lookup, df["company_e_id"] == owner_lookup["_oc_company_id"], "left")
    result = (
        joined
        .withColumn("sales_rep_e_email",
            coalesce(col("orders_e_sales_rep_email"), col("_company_owner_email")))
        .withColumn("sales_rep_e_source",
            when(col("orders_e_sales_rep_email").isNotNull(), lit("order_native"))
            .when(col("_company_owner_email").isNotNull(), lit("company_owner"))
            .otherwise(lit("unresolved")))
    )
    inputs = {"order_rep": col("orders_e_sales_rep_email"),
        "company_id": col("company_e_id"), "matched_company_id": col("_oc_company_id"),
        "owner_id": col("_oc_owner_id"), "owner_email": col("_company_owner_email")}
    if "company_e_id_evidence" in result.columns:
        inputs["company_decision"] = col("company_e_id_evidence")
    return record_evidence(result, spec["_target"], r_rep_native_then_company_owner.strategy_ref,
        col("sales_rep_e_source"), inputs, ctx, result_column="sales_rep_e_email").drop("_oc_company_id", "_oc_owner_id", "_company_owner_email")


@resolve_strategy("owner_id_to_rep_email",
    description="Looks up the company owner ID in the rep dimension and returns the matched email.",
    outcomes={"owner_lookup":"An email was returned by the owner-ID lookup.","unresolved":"The owner-ID lookup returned no email."})
def r_owner_id_to_rep_email(df, spec, ctx):
    """Company dimension: resolve owner_id -> sales_reps.rep_email. The company
    carries a raw HubSpot owner_id (silver, Option B); this joins the sales_reps
    dimension to produce owner_email. Order-derived companies have null owner_id
    -> null owner_email, which the in_hubspot derive then flags false."""
    reps = ctx["dims"]["sales_reps"].select(
        col("sales_rep_e_id").alias("_rid"), col("sales_rep_e_email").alias("_re")
    ).dropDuplicates(["_rid"])
    result = df.join(reps, col("company_e_owner_id") == col("_rid"), "left").withColumn("sales_rep_e_email", col("_re"))
    return record_evidence(result, spec["_target"], r_owner_id_to_rep_email.strategy_ref,
        when(col("_re").isNotNull(), lit("owner_lookup")).otherwise(lit("unresolved")),
        {"owner_id": col("company_e_owner_id"), "matched_rep_id": col("_rid"),
         "matched_email": col("_re")}, ctx, result_column="sales_rep_e_email").drop("_rid", "_re")

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
