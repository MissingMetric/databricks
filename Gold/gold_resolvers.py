# Databricks notebook source
# Gold Resolver Framework
# ─────────────────────────────────────────────────────────────────────────────
# A "resolver" answers one attribution question about an order line, e.g.
#   - which COMPANY is this order for?
#   - which SALES REP gets credit for this order?
#   - is this order a DUPLICATE of one from another platform?
#
# Each question can be answered by multiple STRATEGIES, and the right strategy
# may differ per client and even per source_platform for the same client
# (e.g. Cin7 has a native rep; Shopify must derive one). So a resolver is a
# named strategy; a problem is a family of strategies; and a client config
# selects which strategy to apply (optionally keyed by platform).
#
# Design contract for every strategy function:
#   def strategy(fact_df, dims: dict[str, DataFrame], config: dict) -> DataFrame
#     - takes the working fact (order-line grain) + a dict of dimension tables
#       (companies, contacts, sales_reps, ...) + a config dict
#     - returns the fact with the resolved column(s) ADDED, plus a *_source
#       provenance column recording which strategy produced the value
#     - must be idempotent and must NOT drop or duplicate fact rows
#     - never raises on unresolved -> writes null + source='unresolved'
#     - must tolerate an EMPTY dimension (single-platform client with no CRM):
#       left joins yield nulls and precedence falls through, no crash
#
# CHANGED (Option B): silver no longer resolves hubspot_owner_id -> owner_email.
# Silver carries the RAW owner_id on companies; gold resolves it by joining the
# sales_reps dimension. See company_owner_email_lookup below.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, lower, trim, regexp_replace, coalesce, broadcast,
)

# COMMAND ----------

# ── Strategy registry ────────────────────────────────────────────────────────
# Maps "problem" -> { "strategy_name": fn }. The gold fact notebook looks up the
# strategy named in the client config and applies it. Adding a new strategy is
# registering a function here -- no change to the dispatch logic. This is the
# metadata-driven, no-per-client-code principle applied to resolution.

RESOLVERS: dict[str, dict] = {
    "order_to_company": {},
    "order_to_sales_rep": {},
}

def register(problem: str, name: str):
    """Decorator to register a strategy fn under a problem."""
    def _wrap(fn):
        RESOLVERS[problem][name] = fn
        return fn
    return _wrap

def resolve(problem: str, strategy: str, fact_df: DataFrame, dims: dict, config: dict) -> DataFrame:
    """Dispatch: apply the named strategy for a problem to the fact."""
    if problem not in RESOLVERS:
        raise ValueError(f"Unknown resolver problem: {problem}")
    if strategy not in RESOLVERS[problem]:
        raise ValueError(f"Unknown strategy '{strategy}' for problem '{problem}'. "
                         f"Available: {list(RESOLVERS[problem].keys())}")
    return RESOLVERS[problem][strategy](fact_df, dims, config)

# COMMAND ----------

# ── Shared helper: normalize a company name into a match key ──────────────────
# Lowercase, strip punctuation, drop common suffixes, collapse whitespace. Used
# by multiple company strategies so the normalization is defined once.

def company_match_key(c):
    k = lower(trim(c))
    k = regexp_replace(k, r"[.,]", "")
    k = regexp_replace(k, r"\b(llc|inc|incorporated|corp|co|ltd)\b", "")
    k = regexp_replace(k, r"\s+", " ")
    return trim(k)

# COMMAND ----------

# ── Shared helper: company -> owner rep email (resolves owner_id) ─────────────
# THE OPTION B JOIN. Silver publishes companies.owner_id (raw HubSpot owner id),
# NOT owner_email. Gold resolves it:
#
#     companies.owner_id  ==  sales_reps.rep_id  ->  sales_reps.rep_email
#
# Defined once because BOTH the sales_rep resolver and the companies dimension
# build need it -- one definition means the rule cannot drift between them.
#
# Tolerates an empty/missing sales_reps dimension: the left join yields null
# emails and callers fall through to their next precedence rule.

def company_owner_email_lookup(dims: dict) -> DataFrame:
    """Return a DataFrame of (_oc_company_id, _company_owner_email)."""
    companies = dims["companies"]
    reps = dims["sales_reps"]

    rep_lookup = reps.select(
        col("rep_id").alias("_rep_id"),
        col("rep_email").alias("_rep_email"),
    ).dropDuplicates(["_rep_id"])

    return (
        companies
        .select(
            col("company_id").alias("_oc_company_id"),
            col("owner_id").alias("_oc_owner_id"),
        )
        .join(rep_lookup, col("_oc_owner_id") == col("_rep_id"), "left")
        .select(
            col("_oc_company_id"),
            col("_rep_email").alias("_company_owner_email"),
        )
        .dropDuplicates(["_oc_company_id"])
    )

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# COMPANY RESOLVERS
# ══════════════════════════════════════════════════════════════════════════════

@register("order_to_company", "order_string_only")
def company_order_string_only(fact_df, dims, config):
    """
    Simplest strategy: the company is whatever string is on the order, normalized
    into a canonical key. No matching against HubSpot. Fast, no dimension needed,
    gives correct per-company LTV immediately. company_id == the normalized key.
    """
    return (
        fact_df
        .withColumn("company_key", company_match_key(col("customer_company")))
        .withColumn(
            "company_id",
            when(col("company_key") == "", lit("unresolved"))
            .when(col("company_key").isNull(), lit("unresolved"))
            .otherwise(col("company_key"))
        )
        .withColumn("company_source", lit("order_string_only"))
        .withColumn("company_mismatch", lit(False))
        .drop("company_key")
    )


@register("order_to_company", "hubspot_match_with_fallback")
def company_hubspot_match(fact_df, dims, config):
    """
    Richer strategy with precedence:
      1. Match the order's company string to a HubSpot company (by normalized key)
         -> canonical HubSpot company_id  (company_source = 'hubspot')
      2. Else fall back to the normalized order string as identity
         (company_source = 'order_string')
      3. Else 'unresolved'
    Requires dims['companies']. Records provenance; sets a mismatch flag if the
    order had a string but it didn't match any HubSpot company.

    EMPTY-DIM SAFE: with no companies rows the join matches nothing and every row
    falls through to the order-string branch -- which is exactly right for a
    client with no CRM connected.
    """
    companies = dims["companies"]

    # build a normalized lookup from the companies dimension
    comp_lookup = (
        companies
        .withColumn("_ckey", company_match_key(col("company_name")))
        .select(
            col("company_id").alias("_hs_company_id"),
            col("_ckey"),
        )
        .dropDuplicates(["_ckey"])
    )

    f = fact_df.withColumn("company_key", company_match_key(col("customer_company")))

    joined = f.join(comp_lookup, f["company_key"] == comp_lookup["_ckey"], "left")

    return (
        joined
        .withColumn(
            "company_id",
            coalesce(
                col("_hs_company_id"),                       # 1. HubSpot match
                when(col("company_key") != "", col("company_key")),  # 2. order string
            )
        )
        .withColumn("company_id", coalesce(col("company_id"), lit("unresolved")))  # 3.
        .withColumn(
            "company_source",
            when(col("_hs_company_id").isNotNull(), lit("hubspot"))
            .when(col("company_key") != "", lit("order_string"))
            .otherwise(lit("unresolved"))
        )
        .withColumn(
            "company_mismatch",
            (col("company_key") != "") & col("_hs_company_id").isNull()
        )
        .drop("company_key", "_ckey", "_hs_company_id")
    )

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# SALES REP RESOLVERS
# ══════════════════════════════════════════════════════════════════════════════

@register("order_to_sales_rep", "order_native")
def sales_rep_order_native(fact_df, dims, config):
    """
    The rep is whatever the conformed order already carries (Cin7's
    salesPersonEmail). For platforms with no native rep (Shopify, null), this
    yields null + source='unresolved'. Good default for Cin7-first clients.
    """
    return (
        fact_df
        .withColumn("rep_email", col("sales_rep_email"))
        .withColumn(
            "rep_source",
            when(col("sales_rep_email").isNotNull(), lit("order_native"))
            .otherwise(lit("unresolved"))
        )
    )


@register("order_to_sales_rep", "native_then_company_owner")
def sales_rep_native_then_company_owner(fact_df, dims, config):
    """
    Precedence:
      1. native rep on the order (Cin7 salesPersonEmail)  -> source='order_native'
      2. else the email of the rep who OWNS the order's resolved company,
         resolved owner_id -> sales_reps.rep_email        -> source='company_owner'
      3. else null                                        -> source='unresolved'

    Requires dims['companies'] AND dims['sales_reps'], and that company resolution
    has ALREADY run (needs company_id on the fact). This is why sales_rep resolves
    AFTER company.

    CHANGED (Option B): previously read companies.owner_email directly. Silver no
    longer publishes that column -- it carries owner_id, and the owner->email
    resolution happens here via company_owner_email_lookup.

    EMPTY-DIM SAFE: empty companies/sales_reps -> null owner email -> falls
    through to 'unresolved' without dropping rows.
    """
    owner_lookup = company_owner_email_lookup(dims)

    joined = fact_df.join(
        owner_lookup,
        fact_df["company_id"] == owner_lookup["_oc_company_id"],
        "left",
    )

    return (
        joined
        .withColumn(
            "rep_email",
            coalesce(col("sales_rep_email"), col("_company_owner_email"))
        )
        .withColumn(
            "rep_source",
            when(col("sales_rep_email").isNotNull(), lit("order_native"))
            .when(col("_company_owner_email").isNotNull(), lit("company_owner"))
            .otherwise(lit("unresolved"))
        )
        .drop("_oc_company_id", "_company_owner_email")
    )

# COMMAND ----------

# ── Per-platform dispatch wrapper ────────────────────────────────────────────
# Lets a client apply DIFFERENT strategies per source_platform for the same
# problem (e.g. Cin7 -> 'order_native', Shopify -> 'native_then_company_owner').
# config shape:
#   { "sales_rep": { "default": "order_native",
#                    "by_platform": { "shopify": "native_then_company_owner" } } }

def resolve_per_platform(problem: str, fact_df: DataFrame, dims: dict, config: dict) -> DataFrame:
    """
    Apply possibly-different strategies per source_platform, then union the
    results back together. Falls back to a single strategy if no by_platform map.
    """
    pcfg = config.get(problem, {})
    default_strategy = pcfg.get("default")
    by_platform = pcfg.get("by_platform", {})

    if not by_platform:
        return resolve(problem, default_strategy, fact_df, dims, config)

    platforms = [r["source_platform"] for r in fact_df.select("source_platform").distinct().collect()]
    parts = []
    for p in platforms:
        strat = by_platform.get(p, default_strategy)
        slice_df = fact_df.filter(col("source_platform") == p)
        parts.append(resolve(problem, strat, slice_df, dims, config))

    out = parts[0]
    for part in parts[1:]:
        out = out.unionByName(part, allowMissingColumns=True)
    return out

# COMMAND ----------

print("Resolver framework loaded.")
print("Registered problems and strategies:")
for prob, strats in RESOLVERS.items():
    print(f"  {prob}: {list(strats.keys())}")