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
        .select(col("_oc_company_id"), col("_rep_email").alias("_company_owner_email"))
        .dropDuplicates(["_oc_company_id"])
    )

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1 -- DEDUP strategies
# ══════════════════════════════════════════════════════════════════════════════

@dedup_strategy("none")
def dedup_none(df, spec, ctx):
    """No dedup -- table is already at its own grain."""
    return df


@dedup_strategy("keep_first")
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

@resolve_strategy("company_order_string_only")
def r_company_order_string_only(df, spec, ctx):
    return (
        df
        .withColumn("company_key", company_match_key(col("orders_e_customer_company")))
        .withColumn("company_e_id",
            when(col("company_key") == "", lit("unresolved"))
            .when(col("company_key").isNull(), lit("unresolved"))
            .otherwise(col("company_key")))
        .withColumn("company_e_source", lit("order_string_only"))
        .withColumn("company_e_mismatch", lit(False))
        .drop("company_key")
    )


@resolve_strategy("company_hubspot_match_with_fallback")
def r_company_hubspot_match(df, spec, ctx):
    companies = ctx["dims"]["companies"]
    comp_lookup = (
        companies
        .withColumn("_ckey", company_match_key(col("company_e_name")))
        .select(col("company_e_id").alias("_hs_company_id"), col("_ckey"))
        .dropDuplicates(["_ckey"])
    )
    f = df.withColumn("company_key", company_match_key(col("orders_e_customer_company")))
    joined = f.join(comp_lookup, f["company_key"] == comp_lookup["_ckey"], "left")
    return (
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
        .drop("company_key", "_ckey", "_hs_company_id")
    )


@resolve_strategy("sales_rep_order_native")
def r_rep_order_native(df, spec, ctx):
    return (
        df
        .withColumn("sales_rep_e_email", col("orders_e_sales_rep_email"))
        .withColumn("sales_rep_e_source",
            when(col("orders_e_sales_rep_email").isNotNull(), lit("order_native"))
            .otherwise(lit("unresolved")))
    )


@resolve_strategy("sales_rep_native_then_company_owner")
def r_rep_native_then_company_owner(df, spec, ctx):
    owner_lookup = company_owner_email_lookup(ctx["dims"])
    joined = df.join(owner_lookup, df["company_e_id"] == owner_lookup["_oc_company_id"], "left")
    return (
        joined
        .withColumn("sales_rep_e_email",
            coalesce(col("orders_e_sales_rep_email"), col("_company_owner_email")))
        .withColumn("sales_rep_e_source",
            when(col("orders_e_sales_rep_email").isNotNull(), lit("order_native"))
            .when(col("_company_owner_email").isNotNull(), lit("company_owner"))
            .otherwise(lit("unresolved")))
        .drop("_oc_company_id", "_company_owner_email")
    )


@resolve_strategy("owner_id_to_rep_email")
def r_owner_id_to_rep_email(df, spec, ctx):
    """Company dimension: resolve owner_id -> sales_reps.rep_email. The company
    carries a raw HubSpot owner_id (silver, Option B); this joins the sales_reps
    dimension to produce owner_email. Order-derived companies have null owner_id
    -> null owner_email, which the in_hubspot derive then flags false."""
    reps = ctx["dims"]["sales_reps"].select(
        col("sales_rep_e_id").alias("_rid"), col("sales_rep_e_email").alias("_re")
    ).dropDuplicates(["_rid"])
    return (df.join(reps, col("company_e_owner_id") == col("_rid"), "left")
              .withColumn("sales_rep_e_email", col("_re"))
              .drop("_rid", "_re"))

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

@metric_strategy("sum")
def m_sum(df, m, ctx):
    return spark_sum(m["of"]).over(_win(m))

@metric_strategy("min")
def m_min(df, m, ctx):
    return spark_min(m["of"]).over(_win(m))

@metric_strategy("max")
def m_max(df, m, ctx):
    return spark_max(m["of"]).over(_win(m))

@metric_strategy("count_distinct")
def m_count_distinct(df, m, ctx):
    # size(collect_set(...)) over a window -- distinct count without collapsing rows
    return size(collect_set(m["of"]).over(_win(m)))

@metric_strategy("first_ordered")
def m_first_ordered(df, m, ctx):
    return first(m["of"]).over(_win_ordered(m))

@metric_strategy("cohort_month")
def m_cohort_month(df, m, ctx):
    # yyyy-MM of the earliest order_date in the partition
    return date_format(spark_min(m["of"]).over(_win(m)), "yyyy-MM")

@metric_strategy("days_since")
def m_days_since(df, m, ctx):
    return datediff(current_date(), spark_max(m["of"]).over(_win(m)))

@metric_strategy("is_current_month_of")
def m_is_current_month(df, m, ctx):
    # true if this row's order_date month == the cohort month column named in `of`
    return date_trunc("month", col(m["order_date"])) == col(m["of"])

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4 -- ENRICH strategies
# ══════════════════════════════════════════════════════════════════════════════

@enrich_strategy("left_join_bring")
def e_left_join_bring(df, source_surface, spec, ctx):
    """Left-join a source table's public surface on `on`, bring renamed columns.
    `bring` is {source_col: target_col}. Collisions avoided by explicit renaming."""
    on = spec["on"]
    bring = spec["bring"]
    src_on = spec.get("source_on", on)  # source key col if named differently

    selects = [col(src_on).alias(f"_e_{src_on}")]
    for scol, tcol in bring.items():
        selects.append(col(scol).alias(tcol))
    src = source_surface.select(*selects).dropDuplicates([f"_e_{src_on}"])

    return (df.join(src, df[on] == src[f"_e_{src_on}"], "left")
              .drop(f"_e_{src_on}"))
    
@enrich_strategy("full_join_bring")
def e_full_join_bring(df, source_surface, spec, ctx):
    """Full-join a source table's public surface on `on`, 
    bring renamed columns."""
    on = spec["on"]
    bring = spec["bring"]
    src_on = spec.get("source_on", on)  # source key col if named differently

    selects = [col(src_on).alias(f"_e_{src_on}")]
    for scol, tcol in bring.items():
        selects.append(col(scol).alias(tcol))
    src = source_surface.select(*selects).dropDuplicates([f"_e_{src_on}"])

    return (df.join(src, df[on] == src[f"_e_{src_on}"], "full_outer")
              .drop(f"_e_{src_on}"))

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4 tail -- DERIVE strategies (post-enrich per-row expressions)
# May reference any column on the row (native/resolved/metric/enriched). Terminal.
# ══════════════════════════════════════════════════════════════════════════════

@derive_strategy("coalesce")
def d_coalesce(df, d, ctx):
    """First non-null across `of` (a list of column names). Display-name fallback:
    coalesce(hubspot_name, order_string_name)."""
    return coalesce(*[col(c) for c in d["of"]])


@derive_strategy("is_not_null")
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