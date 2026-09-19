# Databricks notebook source
# Databricks notebook source
"""
Silver pipeline engine (PURE: config in -> silver out).

run(spark, config) executes a resolved, validated config. It does NO I/O to the
repo or Supabase -- the config it receives is already merged, filtered, and
validated. It trusts the dict.

Per enabled entity:
    EXTRACT  -- read bronze JSON for the entity's `source`, according to `shape`
                (header = flatten directly; line = explode + node + flatten)
    CONFORM  -- apply the column map via resolvers.conform
Then across entities:
    RECONCILE -- run each reconciliation (parent_col sum == child_col sum, tolerance)
    WRITE     -- overwrite per-platform silver
    MERGE     -- idempotent upsert into combined, keyed on merge_key

The generic engine + metadata replace the per-platform notebook. `platform` and the
bronze path come from the config, so nothing here is Shopify-specific.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, explode, abs as spark_abs, sum as spark_sum,
)
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

# NOTE: under Databricks %run all modules share ONE namespace, so `conform` and
# the TRANSFORMS registry come from ./resolvers being %run BEFORE this notebook.
# (No import here -- there is no importable module under %run.)


# ── flattening (unchanged from the notebook, platform-agnostic) ──────────────────

def flatten_struct_columns(df: DataFrame, separator: str = "_") -> DataFrame:
    """Recursively flattens StructType columns into `parent_child`. Arrays untouched."""
    while True:
        struct_fields = [
            f.name for f in df.schema.fields
            if isinstance(f.dataType, StructType)
        ]
        if not struct_fields:
            break
        for field_name in struct_fields:
            struct_type = df.schema[field_name].dataType
            nested_cols = [
                col(f"{field_name}.{nested.name}").alias(f"{field_name}{separator}{nested.name}")
                for nested in struct_type.fields
            ]
            other_cols = [c for c in df.columns if c != field_name]
            df = df.select(*other_cols, *nested_cols)
    return df


# ── path builders (platform comes from config, not a constant) ───────────────────

class Paths:
    def __init__(self, slug: str, storage_account: str, platform: str):
        self.slug = slug
        self.sa = storage_account
        self.platform = platform

    def _root(self):
        return f"abfss://{self.slug}@{self.sa}.dfs.core.windows.net"

    def bronze(self, name):
        return f"{self._root()}/bronze/{self.platform}/{name}/landing/data.json"

    def silver(self, name):
        return f"{self._root()}/silver/platforms/{self.platform}/{name}/"

    def combined(self, name):
        return f"{self._root()}/silver/combined/{name}_combined/"


# ── extraction SHAPES ──────────────────────────────────────────────────────────
# `shape` describes HOW rows come out of the bronze JSON -- a separate axis from
# `table` (which common-model contract the output must match). Many tables share a
# shape: order, customer, product are all 'object' (top-level objects); order_line
# is 'nested_array' (exploded from a parent). Adding a new TABLE never needs a new
# shape -- you only add a shape for a genuinely new extraction structure (rare).

def _read_json(spark, path, results_key="results"):
    raw = spark.read.option("multiline", "true").json(path)
    if results_key in raw.columns:
        return raw.select(explode(col(results_key)).alias("_record")).select("_record.*")
    return raw


def extract_object(spark, paths: Paths, ent: dict) -> DataFrame:
    """shape 'object': rows ARE the top-level objects in the source file. Flatten
    directly, then drop any configured columns. Used by order, customer, product."""
    df = _read_json(spark, paths.bronze(ent["source"]))
    df = flatten_struct_columns(df)
    for c in ent.get("drop", []):
        if c in df.columns:
            df = df.drop(c)
    return df


def extract_nested_array(spark, paths: Paths, ent: dict) -> DataFrame:
    """shape 'nested_array': rows live in an array nested inside each top-level
    object. Explode it, optionally unwrap a per-element sub-object (`node_path`),
    carry parent cols down, then flatten. `node_path` is OPTIONAL: Shopify's
    GraphQL connection wraps each element in `node` (node_path: "node"); Cin7's
    lineItems and REST-style arrays have the fields directly on the element (no
    node_path). One generic extractor, one parameter of difference."""
    raw = _read_json(spark, paths.bronze(ent["source"]))

    # parent_cols maps raw-parent-field -> alias to carry into the child rows
    parent_selects = [
        col(raw_field).alias(alias)
        for raw_field, alias in ent.get("parent_cols", {}).items()
    ]

    exploded = raw.select(
        *parent_selects,
        explode(col(ent["explode"])).alias("_el"),
    )

    carried = [alias for _, alias in ent.get("parent_cols", {}).items()]
    node_path = ent.get("node_path")  # optional
    if node_path:
        df = exploded.select(*carried, col(f"_el.{node_path}.*"))   # Shopify
    else:
        df = exploded.select(*carried, col("_el.*"))                # Cin7 / REST

    return flatten_struct_columns(df)


# registry of extraction shapes -> extractor. Add an entry only for a genuinely
# new source structure, not for a new table.
SHAPES = {
    "object": extract_object,
    "nested_array": extract_nested_array,
}


def extract(spark, paths: Paths, ent: dict) -> DataFrame:
    shape = ent["shape"]
    fn = SHAPES.get(shape)
    if fn is None:
        raise ValueError(f"unknown shape '{shape}'. Known: {sorted(SHAPES)}")
    return fn(spark, paths, ent)


# ── reconciliation ───────────────────────────────────────────────────────────────

def run_reconciliation(conformed: dict, r: dict):
    """conformed: {entity_name: DataFrame}. Assert sum(child_col) == parent_col per
    join key, within tolerance. Raises on mismatch (refuse to publish)."""
    parent = conformed[r["parent"]]
    child = conformed[r["child"]]
    tol = r.get("tolerance", 0.01)

    # `on` is either a single column name shared by both sides, or a
    # {"parent": <col>, "child": <col>} pair when the join key is named
    # differently on each (e.g. after bare-naming, order.id == order_line.order_id).
    on = r["on"]
    if isinstance(on, dict):
        p_on, c_on = on["parent"], on["child"]
    else:
        p_on = c_on = on

    child_sums = (
        child.groupBy(c_on)
        .agg(spark_sum(r["child_col"]).alias("_lines_sum"))
        .withColumnRenamed(c_on, "_recon_key")
    )
    recon = (
        parent.select(col(p_on).alias("_recon_key"), r["parent_col"])
        .join(child_sums, on="_recon_key", how="left")
        .withColumn("_diff", spark_abs(col(r["parent_col"]) - col("_lines_sum")))
    )
    mismatches = recon.filter(col("_diff") > tol)
    n = mismatches.count()
    if n > 0:
        mismatches.orderBy(col("_diff").desc()).show(20, truncate=False)
        raise ValueError(
            f"Reconciliation {r['parent']}<->{r['child']} FAILED for {n} key(s): "
            f"sum({r['child_col']}) != {r['parent_col']} within {tol}. "
            f"Refusing to publish silver."
        )
    print(f"✓ reconciliation {r['parent']}<->{r['child']} passed ({recon.count()} keys)")


# ── write + merge ────────────────────────────────────────────────────────────────

def write_silver(df: DataFrame, paths: Paths, name: str):
    (df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(paths.silver(name)))
    print(f"✓ {name} written to silver/platforms/{paths.platform}/{name}/")


def merge_combined(spark, df: DataFrame, paths: Paths, name: str, merge_key: list):
    target = paths.combined(name)
    if DeltaTable.isDeltaTable(spark, target):
        cond = " AND ".join(f"t.{k} = s.{k}" for k in merge_key)
        tgt = DeltaTable.forPath(spark, target)
        (tgt.alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        print(f"✓ merged {name} into combined/{name}_combined/ (key: {merge_key})")
    else:
        df.write.format("delta").mode("overwrite").save(target)
        print(f"✓ created combined/{name}_combined/")


# ── orchestration ────────────────────────────────────────────────────────────────

def run(spark, config: dict, storage_account: str):
    """Execute a resolved, validated config. config must carry slug, platform,
    entities (filtered), and reconciliations."""
    slug = config["slug"]
    platform = config["platform"]
    paths = Paths(slug, storage_account, platform)
    entities = config["entities"]

    print(f"Running {platform} silver for [{slug}] :: entities = {list(entities)}")

    # EXTRACT + CONFORM every enabled entity, holding conformed frames for recon.
    conformed = {}
    for name, ent in entities.items():
        flat = extract(spark, paths, ent)
        out = conform(flat, ent["columns"])
        conformed[name] = out
        print(f"  conformed {name}: {len(out.columns)} cols")

    # RECONCILE (only recons whose entities survived the filter reach here).
    for r in config.get("reconciliations", []):
        run_reconciliation(conformed, r)

    # WRITE + MERGE.
    for name, ent in entities.items():
        df = conformed[name]
        write_silver(df, paths, name)
        merge_combined(spark, df, paths, name, ent["merge_key"])

    print(f"✓ {platform} silver complete for [{slug}]")
    return conformed