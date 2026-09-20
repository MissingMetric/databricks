# Databricks notebook source
# Gold build -- single entry point for the metadata-driven gold layer
# ─────────────────────────────────────────────────────────────────────────────
# Replaces the old gold_orders_fact + gold_dimensions notebooks. Everything
# table-specific lives in gold_tables.json; this notebook is just plumbing:
#
#   1. load engine + strategies + validator (via %run)
#   2. load gold table metadata + per-client resolution overrides (Supabase)
#   3. apply overrides onto the metadata's resolve strategy names (base+override)
#   4. VALIDATE the merged metadata before any Spark work
#   5. wire load_fn per table (orders = lines⋈headers; companies = orders surface
#      + HubSpot attrs) and run the five-stage engine
#   6. write each gold table
#
# Adding a table or changing resolution = editing metadata/config, not this file.

# COMMAND ----------

# MAGIC %run ./gold_engine

# COMMAND ----------

# MAGIC %run ./gold_strategies

# COMMAND ----------

# MAGIC %run ./gold_validate

# COMMAND ----------

# MAGIC %run ./gold_catalog

# COMMAND ----------

import copy
import json
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, lower, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# COMMAND ----------

# ── Parameters ──────────────────────────────────────────────────────────────────
dbutils.widgets.text("slug", "development", "Client Slug")
slug = dbutils.widgets.get("slug")
if not slug:
    raise ValueError("slug parameter is required")

storage_account = dbutils.secrets.get(scope="kv", key="ADLS-STORAGE-ACCOUNT")
supabase_url = dbutils.secrets.get(scope="kv", key="SUPABASE-URL")
supabase_key = dbutils.secrets.get(scope="kv", key="SUPABASE-SECRET")

CONFIG_CONTAINER = "configs"

def combined_path(name): return f"abfss://{slug}@{storage_account}.dfs.core.windows.net/silver/combined/{name}_combined/"
def gold_path(name):     return f"abfss://{slug}@{storage_account}.dfs.core.windows.net/gold/{name}/"
def config_path(rel):    return f"abfss://{CONFIG_CONTAINER}@{storage_account}.dfs.core.windows.net/{rel}"

print(f"Building gold for: {slug}")

# COMMAND ----------

# ── Config loading ───────────────────────────────────────────────────────────────

def download_json(path):
    rows = spark.read.option("wholetext", "true").text(path).collect()
    return json.loads("".join(r.value for r in rows))


def deep_merge(base, override):
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def fetch_client_resolution(slug):
    """Per-client resolution override from client_configs. {} = pure metadata
    defaults. Uses PostgREST directly (no supabase SDK -- it rejects the
    sb_secret_ key format)."""
    import requests
    resp = requests.get(
        f"{supabase_url.rstrip('/')}/rest/v1/client_configs_by_slug",
        headers={"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}"},
        params={"client_slug": f"eq.{slug}", "select": "resolution_config"},
        timeout=30,
    )
    resp.raise_for_status()
    rows = resp.json()
    return (rows[0].get("resolution_config") or {}) if rows else {}

# COMMAND ----------

# ── Load metadata + apply per-client resolution overrides ────────────────────────
# gold_tables.json declares each table's DEFAULT resolve strategies. The client's
# resolution_config overrides strategy names per problem, e.g.
#   { "orders": { "resolve": { "rep_email": { "strategy": "..." } } } }
# Same base+override model as silver. Only the strategy names are overridable;
# structure (which tables, which metrics) is fixed in the metadata.

gold_meta = download_json(config_path("Gold/gold_tables.json"))
tables_meta = {k: v for k, v in gold_meta.items() if not k.startswith("_")}

resolution_override = fetch_client_resolution(slug)
if resolution_override:
    tables_meta = deep_merge(tables_meta, resolution_override)
    print(f"Applied client resolution override: {json.dumps(resolution_override)}")
else:
    print("No client override -- using metadata default strategies")

# COMMAND ----------

# ── VALIDATE merged metadata before any Spark work ───────────────────────────────
# Catches unregistered strategies, metrics grouping over non-identity columns,
# enrichment pulling columns outside a source table's public surface.

surfaces = validate_gold(tables_meta, DEDUP, RESOLVE, METRIC, ENRICH, DERIVE)
print("✓ gold metadata valid")
for name, surf in surfaces.items():
    print(f"  {name}: {len(surf)}-col public surface")

# COMMAND ----------

# ── Shared silver cache ──────────────────────────────────────────────────────────
# Every silver combined table is read AT MOST ONCE and cached. Both consumers pull
# from here:
#   * the generic loader (a gold table's own `source` tables)
#   * ctx["dims"] (raw dimensions the RESOLVERS join against)
# so companies/sales_reps aren't read twice just because they're both a gold
# table's source AND a resolve-time dimension.
#
# IMPORTANT (the raw-vs-surface distinction): ctx["dims"] holds the RAW silver
# form -- company_id/company_name/owner_id as silver produced them. That is what
# resolvers match against. It is NOT the built gold surface (with LTV etc.), which
# only exists after stage 3 and lives in ctx["surfaces"]. Same entity, different
# life-stage; resolve-time wants the raw one.

_TYPES = {"string": StringType(), "double": DoubleType(), "timestamp": TimestampType()}
common_model = download_json(config_path("Common/common_model.json"))

def schema_for_table(table):
    cols = common_model["tables"][table]["columns"]
    return StructType([StructField(n, _TYPES.get(t, StringType()), True) for n, t in cols.items()])

_SILVER = {}
def read_combined(name):
    """Read a silver combined table once; cache it. Returns None if absent."""
    if name not in _SILVER:
        try:
            _SILVER[name] = spark.read.format("delta").load(combined_path(name))
        except Exception:
            _SILVER[name] = None
    return _SILVER[name]

def dim_or_empty(name, table):
    """Raw silver dimension for ctx, or a correctly-typed empty frame if absent
    (single-platform client). Uses the shared cache."""
    df = read_combined(name)
    if df is None:
        print(f"  ! {name}: no combined table -- empty dimension")
        return spark.createDataFrame([], schema_for_table(table))
    return df

# COMMAND ----------

# ── ctx ──────────────────────────────────────────────────────────────────────────
# ctx['dims'] is NO LONGER hand-loaded here. run_gold populates it from each
# table's POST-DEDUP frame after the stage-1 barrier, so resolvers join against
# deduped dimensions, never a raw silver read. Every dimension a resolver needs
# (companies, sales_reps) must therefore be a gold table in the metadata -- even
# if export:false (built + used, not written).
ctx = {}

# COMMAND ----------

# ── Generic loader: every gold table starts at silver table(s) ───────────────────
# The `source` spec declares which silver combined tables to load and how to join
# them, pulled from the SHARED CACHE (so a table that's also a resolve dimension
# isn't re-read). No per-table load code -- adding a gold table sourced from silver
# is pure metadata.
#
#   source: { "tables": ["order_line_items","orders"], "on": [...], "how":"inner" }
#   source: { "tables": ["companies"] }   # single table, no join

def load_table(tm):
    s = tm["source"]
    dfs = [read_combined(t) for t in s["tables"]]
    missing = [t for t, d in zip(s["tables"], dfs) if d is None]
    if missing:
        raise FileNotFoundError(
            f"gold table source needs silver combined table(s) {missing}, which "
            f"are absent. A fact source must exist; check the silver run."
        )
    out = dfs[0]
    for d in dfs[1:]:
        out = _join_on(out, d, s["on"], s.get("how", "inner"))
    return out


def _join_on(left, right, on, how):
    """Join supporting keys named differently on each side. `on` is a list whose
    entries are either:
      - a string  -> a column shared by both sides (USING-style), or
      - {"left": L, "right": R} -> different names (e.g. line.order_id == header.id).
    Right-side join keys are renamed to unique temps before the join (so same-named
    columns like `id` on both sides don't make the condition ambiguous), then the
    temps are dropped after -- leaving the left side's column as canonical.
    """
    shared = [k for k in on if isinstance(k, str)]
    pairs = [k for k in on if isinstance(k, dict)]

    if not pairs:
        return left.join(right, on=shared, how=how)

    r2 = right
    conds = []
    temps = []
    for i, p in enumerate(pairs):
        t = f"_rjk_{i}"
        r2 = r2.withColumnRenamed(p["right"], t)
        conds.append(left[p["left"]] == r2[t]); temps.append(t)
    for k in shared:
        t = f"_rsk_{k}"
        r2 = r2.withColumnRenamed(k, t)
        conds.append(left[k] == r2[t]); temps.append(t)

    cond = conds[0]
    for c in conds[1:]:
        cond = cond & c

    joined = left.join(r2, cond, how)
    for t in temps:
        joined = joined.drop(t)
    return joined

# COMMAND ----------

# ── Run the engine: ALL tables together through the phased barriers ──────────────
# run_gold executes all tables through stage 1, then all through stage 2, etc.
# This is what makes mutual enrichment safe: orders enriches company_name FROM
# companies, companies enriches company_ltv FROM orders -- each reads the other's
# FROZEN stage-3 surface, so there's no cycle. They must run in ONE call for the
# barriers to span them.

results = run_gold(tables_meta, load_table, ctx)

for name, df in results.items():
    print(f"✓ {name}: {df.count()} rows, {len(df.columns)} cols")

# COMMAND ----------

# ── Write outputs (only export != false) ─────────────────────────────────────────
# Every table went through all stages and was available to others via ctx. But
# only tables with export != false get WRITTEN to gold/. An internal dimension
# (e.g. sales_reps, used by resolvers but not a client-facing table) sets
# export:false -- built and used, never written.
written, internal = [], []
# Fail before replacing any data if generated evidence and metadata disagree.
build_full_catalog(tables_meta, strategies=STRATEGY_DEFINITIONS,
                   schema_columns={name: df.columns for name, df in results.items()}, run_id=ctx["run_id"])
for name, df in results.items():
    if tables_meta[name].get("export", True):
        df.write.mode("overwrite").parquet(gold_path(name).rstrip("/"))
        written.append(name)
    else:
        internal.append(name)

for n in written:  print(f"✓ wrote gold/{n}/")
for n in internal: print(f"· {n}: internal (export:false) -- built, not written")

# ── Emit the field catalog ───────────────────────────────────────────────────
# The catalog is authored in each table's `catalog` block (label + dataType per
# output column). Build it and write catalog.json into the gold container, next
# to the tables, so the API reads it exactly like it reads gold parquet -- and it
# refreshes on the same pipeline run. This replaces the API's DESCRIBE + type
# inference: types are declared where columns are born, not guessed downstream.
catalog_out = f"abfss://{slug}@{storage_account}.dfs.core.windows.net/gold/catalog.json"
write_catalog(tables_meta, catalog_out, strategies=STRATEGY_DEFINITIONS,
              schema_columns={name: df.columns for name, df in results.items()}, run_id=ctx["run_id"])

# COMMAND ----------

print(f"""
gold build complete for [{slug}]
──────────────────────────────────────────────
  written:  {written}
  internal: {internal}
  engine:   dedup -> resolve -> metrics -> enrich -> derive (metadata-driven)
""")
