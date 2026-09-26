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
from pyspark.sql.functions import col, lit, row_number, struct, to_json, array, concat, when

# COMMAND ----------
# MAGIC %run ./gold_resolver_config
# COMMAND ----------
# MAGIC %run ./gold_dedup
# COMMAND ----------
from datetime import datetime, timezone
from uuid import uuid4

# COMMAND ----------

# ── The four registries ──────────────────────────────────────────────────────

DEDUP    = {}   # name -> fn(df, spec, ctx) -> df
RESOLVE  = {}   # name -> fn(df, spec, ctx) -> df   (adds identity/measure cols + *_source)
METRIC   = {}   # name -> fn(df, spec, ctx) -> Column   (one aggregated column)
ENRICH   = {}   # name -> fn(df, source_surface_df, spec, ctx) -> df
DERIVE   = {}   # name -> fn(df, spec, ctx) -> Column   (post-enrich per-row expression)

STRATEGY_DEFINITIONS = {}

def _reg(registry, name, description, outcomes=None, inputs=None, version=1):
    def _wrap(fn):
        if name in registry:
            raise ValueError(f"{name} already registered")
        registry[name] = fn
        stage = next(k for k, v in {"dedup": DEDUP, "resolve": RESOLVE,
            "metric": METRIC, "enrich": ENRICH, "derive": DERIVE}.items() if v is registry)
        ref = f"{stage}.{name}@{version}"
        fn.strategy_ref = ref
        STRATEGY_DEFINITIONS[ref] = {"id": ref, "name": name, "stage": stage,
            "version": version, "description": description,
            "outcomes": outcomes or {}, "inputs": inputs or {}}
        return fn
    return _wrap

def dedup_strategy(name, **definition): return _reg(DEDUP, name, **definition)
def resolve_strategy(name, row_inputs=None, **definition):
    def register(fn):
        fn.row_inputs = row_inputs or {}
        fn = _reg(RESOLVE, name, inputs={
            key: {"origin": "row", "default_field": field}
            for key, field in fn.row_inputs.items()
        }, **definition)(fn)
        return fn
    return register
def metric_strategy(name, **definition): return _reg(METRIC, name, **definition)
def enrich_strategy(name, **definition): return _reg(ENRICH, name, **definition)
def derive_strategy(name, **definition): return _reg(DERIVE, name, **definition)


def record_evidence(df, target, ref, source, inputs, ctx, result_column=None):
    """Capture values BEFORE scratch columns are dropped. JSON preserves nulls
    and has one schema across platform-specific strategies and Parquet exports.
    Evidence is per application/output, never keyed by strategy name alone.
    """
    if result_column and result_column != target:
        df = df.withColumn(target, col(result_column))
    context = [lit(ref).alias("strategy"), lit(target).alias("field"),
        lit(ctx["run_id"]).alias("run_id"), lit(ctx["processed_at"]).alias("processed_at"),
        source.alias("source"), col(target).alias("result"),
        struct(*[value.alias(key) for key, value in inputs.items()]).alias("inputs")]
    return (df.withColumn(target + "_source", source)
        .withColumn(target + "_evidence", to_json(struct(*context), {"ignoreNullFields": "false"})))

# COMMAND ----------

# ── Stage runners ────────────────────────────────────────────────────────────
# Each stage reads its slice of the table's metadata and dispatches to registered
# strategies. `ctx` carries shared context (dims, other tables' surfaces, config).

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
        if c in system or "_e_" in c or c.startswith("__mm_"):
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
    return run_dedup_pipeline(df, table_meta, ctx)


def run_resolve(df: DataFrame, table_meta: dict, ctx: dict) -> DataFrame:
    resolvers = table_meta.get("resolve", {})
    for target in resolver_order(resolvers, RESOLVE, df.columns):
        variants = resolver_variants(resolvers[target])
        if len(variants) == 1:
            df = _resolve_chain(df, variants[None], ctx, target)
            continue
        if "source_platform" not in df.columns:
            raise ValueError("Platform-specific chains require source_platform")
        platforms = [r[0] for r in df.select("source_platform").distinct().collect()]
        parts = [_resolve_chain(df.filter(col("source_platform").eqNullSafe(lit(p))),
                    variants.get(p, variants[None]), ctx, target) for p in platforms]
        if not parts:
            df = _resolve_chain(df, variants[None], ctx, target)
        else:
            df = parts[0]
            for part in parts[1:]:
                df = df.unionByName(part)
    return df


def _resolve_chain(df, steps, ctx, target):
    """First success wins. Only unresolved rows are passed to the next piece.

    Candidate strategies may add only __mm_candidate. They must preserve every
    input row and return a string identity/value, status, reason and JSON inputs.
    Ambiguous matches fail the build; they are never silently treated as no match.
    """
    scratch = ["__mm_attempts", "__mm_winner", "__mm_strategy", "__mm_reason",
               "__mm_value", "__mm_candidate"]
    if set(scratch) & set(df.columns):
        raise ValueError("Reserved resolver scratch column collision")
    attempt_type = "array<struct<step:string,strategy:string,status:string,reason:string,inputs:string>>"
    pending = (df.withColumn("__mm_attempts", array().cast(attempt_type))
        .withColumn("__mm_winner", lit(None).cast("string"))
        .withColumn("__mm_strategy", lit(None).cast("string"))
        .withColumn("__mm_reason", lit("unresolved"))
        .withColumn("__mm_value", lit(None).cast("string")))
    resolved = []
    for step in steps:
        fn = RESOLVE[step["strategy"]]
        params = {**fn.row_inputs, **step["params"]}
        candidate = fn(pending, params, ctx)
        if set(candidate.columns) != set(pending.columns) | {"__mm_candidate"}:
            raise ValueError(f"{step['id']}: strategy changed the input column contract")
        # Multiset comparisons catch duplicated, removed AND modified input rows.
        original = candidate.select(*pending.columns)
        if (original.exceptAll(pending).limit(1).count()
                or pending.exceptAll(original).limit(1).count()):
            raise ValueError(f"{step['id']}: strategy did not preserve input rows")
        fields = {f.name: f.dataType.simpleString() for f in candidate.schema["__mm_candidate"].dataType.fields}
        if fields != {k: "string" for k in ("value", "status", "reason", "inputs")}:
            raise ValueError(f"{step['id']}: invalid candidate schema")
        c = col("__mm_candidate")
        invalid = (c.isNull() | c.status.isNull() | c.reason.isNull() | c.inputs.isNull()
            | ~c.status.isin("resolved", "no_match")
            | ((c.status == "resolved") & c.value.isNull())
            | ((c.status == "no_match") & c.value.isNotNull()))
        if candidate.filter(invalid).limit(1).count():
            raise ValueError(f"{step['id']}: ambiguous lookup or invalid candidate; resolution stopped")
        attempt = struct(lit(step["id"]).alias("step"), lit(fn.strategy_ref).alias("strategy"),
            c.status.alias("status"), c.reason.alias("reason"), c.inputs.alias("inputs"))
        candidate = candidate.withColumn("__mm_attempts", concat(col("__mm_attempts"), array(attempt)))
        won = (candidate.filter(c.status == "resolved")
            .withColumn("__mm_winner", lit(step["id"]))
            .withColumn("__mm_strategy", lit(fn.strategy_ref))
            .withColumn("__mm_reason", c.reason)
            .withColumn("__mm_value", c.value).drop("__mm_candidate"))
        resolved.append(won)
        pending = candidate.filter(c.status == "no_match").drop("__mm_candidate")
    out = pending
    for part in resolved:
        out = out.unionByName(part)
    out = out.withColumn(target, col("__mm_value")).withColumn(target + "_source", col("__mm_reason"))
    envelope = struct(lit(2).alias("evidence_version"), lit(target).alias("field"),
        col("__mm_strategy").alias("strategy"), col("__mm_winner").alias("winner_step"),
        when(col("__mm_winner").isNotNull(), lit("resolved")).otherwise(lit("unresolved")).alias("status"),
        col("__mm_reason").alias("source"), col(target).alias("result"),
        lit(ctx["run_id"]).alias("run_id"), lit(ctx["processed_at"]).alias("processed_at"),
        col("__mm_attempts").alias("attempts"))
    return out.withColumn(target + "_evidence", to_json(envelope, {"ignoreNullFields": "false"})).drop(*scratch)


def run_metrics(df: DataFrame, table_meta: dict, ctx: dict) -> DataFrame:
    """Add each metric column. A metric names an agg strategy + the identity column
    to group/partition over. Declarative aggregates and windowed metrics are both
    just registered strategies returning a Column."""
    for m in table_meta.get("metrics", []):
        strat = m["agg"]
        roles = [k for k in ("of", "over", "order_by", "order_date") if m.get(k)]
        for role in roles:
            df = df.withColumn(f"__mm_metric_{role}", col(m[role]))
        df = df.withColumn(m["name"], METRIC[strat](df, m, ctx))
        inputs = {role: col(f"__mm_metric_{role}") for role in roles}
        df = record_evidence(df, m["name"], METRIC[strat].strategy_ref, lit("calculated"), inputs, ctx)
        df = df.drop(*[f"__mm_metric_{role}" for role in roles])
    return df


def run_enrich(df: DataFrame, table_meta: dict, ctx: dict) -> DataFrame:
    """Join out to each source table's public surface and bring display columns."""
    for spec in table_meta.get("enrich", []):
        spec = {"strategy": "left_join_bring", **spec}
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
        of = d.get("of", [])
        keys = of if isinstance(of, list) else [of]
        for index, key in enumerate(keys):
            df = df.withColumn(f"__mm_derive_{index}", col(key))
        df = df.withColumn(d["name"], DERIVE[strat](df, d, ctx))
        inputs = {key: col(f"__mm_derive_{index}") for index, key in enumerate(keys)}
        df = record_evidence(df, d["name"], DERIVE[strat].strategy_ref, lit("derived"), inputs, ctx)
        df = df.drop(*[f"__mm_derive_{index}" for index in range(len(keys))])
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
    ctx.setdefault("run_id", str(uuid4()))
    ctx.setdefault("processed_at", datetime.now(timezone.utc).isoformat())
    frames = {}

    # STAGE 1: load + dedup (all tables). Nothing is joined against a table until
    # it has been through its OWN dedup -- so after this barrier, publish each
    # table's post-dedup frame into ctx['dims']. Resolvers (stage 2) join against
    # THESE deduped frames, never a raw silver read. This is why the phased barrier
    # matters: every dedup is done before any resolve begins.
    for name in names:
        base = load_fn(tables_meta[name])
        frames[name] = run_dedup(base, {**tables_meta[name], "_name": name}, ctx)
    # Apply foreign-ID aliases only after ALL canonical mappings are available.
    for name in names:
        linked = apply_identity_links(frames[name], tables_meta[name], tables_meta, ctx)
        frames[name] = prefix_native(linked, {**tables_meta[name], "_name": name})
        print(f"  [1 dedup+prefix] {name}")

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
