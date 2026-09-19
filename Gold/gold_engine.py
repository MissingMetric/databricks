# Databricks notebook source
# Gold four-stage engine
# ─────────────────────────────────────────────────────────────────────────────
# Every gold table is built by the SAME four stages, in the same order:
#
#   1. DEDUP    -- collapse to one row per own identity (SKU dedup, order dedup)
#   2. RESOLVE  -- determine this table's foreign keys + any values it will
#                  aggregate over (company_id, product_sku, rep_email, unit_cost).
#                  Produces IDENTITY columns and resolved MEASURES.
#   3. METRICS  -- aggregate this table's own columns, grouped over an identity
#                  column (own PK or a resolved FK). Writes metric columns.
#   4. ENRICH   -- join out to other tables' PUBLIC SURFACE for display columns.
#                  One hop only; brings native + resolved + metric columns, never
#                  another table's enrichments.
#
# The runner executes all tables through stage 1, THEN all through stage 2, etc.
# (global barriers). This makes cross-table dependencies safe without a topo sort:
# by the time any table enriches (stage 4), every table it enriches from has
# finished producing its public surface (stages 1-3).
#
# TWO RULES the validator enforces (see gold_validate):
#   - a metric's `over` must be an IDENTITY column (own PK or resolved FK) --
#     produced by stage 1 or 2, never a measure or an enrichment.
#   - an enrichment may only `bring` a source table's PUBLIC SURFACE
#     (native + resolved + metrics), never that table's own enrichments.
#
# Each stage is a REGISTRY of named strategies, same pattern as silver transforms
# and the existing resolvers -- adding a strategy is registering a function, not
# changing the engine.

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, row_number

# COMMAND ----------

# ── The four registries ──────────────────────────────────────────────────────

DEDUP    = {}   # name -> fn(df, spec, ctx) -> df
RESOLVE  = {}   # name -> fn(df, spec, ctx) -> df   (adds identity/measure cols + *_source)
METRIC   = {}   # name -> fn(df, spec, ctx) -> Column   (one aggregated column)
ENRICH   = {}   # name -> fn(df, source_surface_df, spec, ctx) -> df
DERIVE   = {}   # name -> fn(df, spec, ctx) -> Column   (post-enrich per-row expression)

def _reg(registry, name):
    def _wrap(fn):
        if name in registry:
            raise ValueError(f"{name} already registered")
        registry[name] = fn
        return fn
    return _wrap

def dedup_strategy(name):  return _reg(DEDUP, name)
def resolve_strategy(name): return _reg(RESOLVE, name)
def metric_strategy(name): return _reg(METRIC, name)
def enrich_strategy(name): return _reg(ENRICH, name)
def derive_strategy(name): return _reg(DERIVE, name)

# COMMAND ----------

# ── Stage runners ────────────────────────────────────────────────────────────
# Each stage reads its slice of the table's metadata and dispatches to registered
# strategies. `ctx` carries shared context (dims, other tables' surfaces, config).

def prefix_native(df: DataFrame, table_meta: dict) -> DataFrame:
    """Rename every NATIVE column (present after load+dedup) to `<entity>_e_<name>`.

    This runs once, right after dedup, BEFORE resolve -- so raw silver columns
    (including raw pre-resolve foreign keys) get THIS table's prefix, because at
    this point they are native to this table's grain. The relationship-establishing
    stages that come later (resolve/metric/enrich) emit their OWN correctly-prefixed
    columns for other entities.

    Exceptions, declared per table:
      - `entity`:         the prefix to use (defaults to the table name)
      - `system_columns`: columns that stay BARE and shared (e.g. source_platform)

    Idempotent-ish: a column already containing `_e_` is left alone, so re-running
    or already-prefixed inputs don't get double-prefixed.
    """
    entity = table_meta.get("entity", table_meta.get("_name"))
    if not entity:
        raise ValueError("prefix_native needs table_meta['entity'] or ['_name']")
def prefix_native(df: DataFrame, table_meta: dict) -> DataFrame:
    """Name native columns from gold metadata (silver is bare). Three cases:

      - grain PK (grain_pk)     -> <entity>_e_id       (the row identity)
      - carried key (declared)  -> <entity>_e_<key>    (source.carried_keys)
      - everything else         -> <entity>_e_<barecol>
      - system columns          -> unchanged (bare, shared)

    No collision handling: the common model is hand-authored, so bare column
    names are kept distinct by design (name the header total `order_total`, the
    line amount `line_revenue`, etc.). The engine doesn't guard against a problem
    the model author simply avoids.
    """
    entity = table_meta.get("entity", table_meta.get("_name"))
    if not entity:
        raise ValueError("prefix_native needs table_meta['entity'] or ['_name']")
    system = set(table_meta.get("system_columns", ["source_platform"]))

    grain_pk = table_meta.get("grain_pk")
    carried = table_meta.get("source", {}).get("carried_keys", {})

    renames = {}
    for c in df.columns:
        if c in system or "_e_" in c:
            continue
        if c == grain_pk:
            renames[c] = f"{entity}_e_id"
        elif c in carried:
            renames[c] = f"{entity}_e_{carried[c]}"
        else:
            renames[c] = f"{entity}_e_{c}"

    for old, new in renames.items():
        df = df.withColumnRenamed(old, new)
    return df


def run_dedup(df: DataFrame, table_meta: dict, ctx: dict) -> DataFrame:
    spec = table_meta.get("dedup")
    if not spec:
        return df
    strat = spec.get("strategy", "keep_first")
    return DEDUP[strat](df, spec, ctx)


def run_resolve(df: DataFrame, table_meta: dict, ctx: dict) -> DataFrame:
    """Apply each foreign-key / measure resolver in declaration order. Each adds
    its identity/measure column(s) and a *_source provenance column."""
    for target, spec in table_meta.get("resolve", {}).items():
        strat = spec["strategy"]
        fn = RESOLVE[strat]
        # per-platform dispatch if the spec declares by_platform
        df = _resolve_maybe_per_platform(fn, df, spec, ctx, target)
    return df


def _resolve_maybe_per_platform(fn, df, spec, ctx, target):
    by_platform = spec.get("by_platform")
    if not by_platform:
        return fn(df, spec, ctx)
    # split by platform, apply the platform's chosen strategy, union back
    default_strat = spec.get("strategy")
    platforms = [r["source_platform"] for r in df.select("source_platform").distinct().collect()]
    parts = []
    for p in platforms:
        strat = by_platform.get(p, default_strat)
        slice_df = df.filter(col("source_platform") == p)
        parts.append(RESOLVE[strat](slice_df, spec, ctx))
    out = parts[0]
    for part in parts[1:]:
        out = out.unionByName(part, allowMissingColumns=True)
    return out


def run_metrics(df: DataFrame, table_meta: dict, ctx: dict) -> DataFrame:
    """Add each metric column. A metric names an agg strategy + the identity column
    to group/partition over. Declarative aggregates and windowed metrics are both
    just registered strategies returning a Column."""
    for m in table_meta.get("metrics", []):
        strat = m["agg"]
        df = df.withColumn(m["name"], METRIC[strat](df, m, ctx))
    return df


def run_enrich(df: DataFrame, table_meta: dict, ctx: dict) -> DataFrame:
    """Join out to each source table's public surface and bring display columns."""
    for spec in table_meta.get("enrich", []):
        source_surface = ctx["surfaces"][spec["from"]]
        strat = spec.get("strategy", "left_join_bring")
        df = ENRICH[strat](df, source_surface, spec, ctx)
    return df


def run_derive(df: DataFrame, table_meta: dict, ctx: dict) -> DataFrame:
    """Stage 4 tail: post-enrich per-row column expressions. Unlike enrichment,
    derived columns may reference ANYTHING on the row (native, resolved, metric,
    enriched) because they are terminal -- consumed by nobody downstream, so they
    never widen another table's surface. This is where display-name fallback
    (coalesce) and boolean flags (in_hubspot) live."""
    for d in table_meta.get("derive", []):
        strat = d["expr"]
        df = df.withColumn(d["name"], DERIVE[strat](df, d, ctx))
    return df

# COMMAND ----------

# ── Public surface computation ───────────────────────────────────────────────
# A table's public surface = columns produced by stages 1-3 (native + resolved +
# metrics), NOT its stage-4 enrichments. This is what other tables may enrich
# from. Computed automatically after stage 3 so there's no separate `publishes`
# list to drift.

def compute_surface(df_after_metrics: DataFrame, table_meta: dict) -> DataFrame:
    """The columns available to downstream enrichment: everything on the frame
    after stage 3. (Enrichment runs in stage 4, after surfaces are frozen, so a
    surface never includes enrichments by construction.)"""
    return df_after_metrics

# COMMAND ----------

# ── The phased runner ────────────────────────────────────────────────────────

def run_gold(tables_meta: dict, load_fn, ctx: dict) -> dict:
    """Execute all gold tables through the four stages with global barriers.

    tables_meta: { table_name: {source, dedup, resolve, metrics, enrich, derive, export} }
    load_fn:     fn(table_meta) -> base DataFrame for that table
    ctx:         shared context. run_gold POPULATES ctx['dims'] (post-dedup frames,
                 after the stage-1 barrier) and ctx['surfaces'] (post-stage-3).
                 Callers pass ctx={} (or with extra shared config); they do NOT
                 hand-load dimensions -- resolvers get deduped frames from ctx['dims'].

    Returns { table_name: final DataFrame } after all stages (including
    export:false tables -- the caller decides what to write).
    """
    names = list(tables_meta)
    frames = {}

    # STAGE 1: load + dedup (all tables). Nothing is joined against a table until
    # it has been through its OWN dedup -- so after this barrier, publish each
    # table's post-dedup frame into ctx['dims']. Resolvers (stage 2) join against
    # THESE deduped frames, never a raw silver read. This is why the phased barrier
    # matters: every dedup is done before any resolve begins.
    for name in names:
        base = load_fn(tables_meta[name])
        deduped = run_dedup(base, tables_meta[name], ctx)
        # tell prefix_native the table name so `entity` can default to it
        meta_with_name = dict(tables_meta[name])
        meta_with_name.setdefault("_name", name)
        frames[name] = prefix_native(deduped, meta_with_name)
        print(f"  [1 dedup+prefix] {name}: native cols -> {name}_e_*")

    # publish deduped (and native-prefixed) dimensions for the resolvers
    ctx["dims"] = dict(frames)

    # STAGE 2: resolve foreign keys (all tables)
    for name in names:
        frames[name] = run_resolve(frames[name], tables_meta[name], ctx)
        print(f"  [2 resolve] {name}")

    # STAGE 3: metrics (all tables) -- then freeze each table's public surface
    ctx["surfaces"] = {}
    for name in names:
        frames[name] = run_metrics(frames[name], tables_meta[name], ctx)
        ctx["surfaces"][name] = compute_surface(frames[name], tables_meta[name])
        print(f"  [3 metrics] {name}: {len(frames[name].columns)} cols (surface frozen)")

    # STAGE 4: enrich then derive (all tables) -- reads other tables' frozen surfaces
    for name in names:
        frames[name] = run_enrich(frames[name], tables_meta[name], ctx)
        frames[name] = run_derive(frames[name], tables_meta[name], ctx)
        print(f"  [4 enrich+derive]  {name}: {len(frames[name].columns)} cols")

    return frames

# COMMAND ----------

print("Gold four-stage engine loaded.")
print(f"  dedup strategies:   {sorted(DEDUP)}")
print(f"  resolve strategies: {sorted(RESOLVE)}")
print(f"  metric strategies:  {sorted(METRIC)}")
print(f"  enrich strategies:  {sorted(ENRICH)}")
print(f"  derive strategies:  {sorted(DERIVE)}")
