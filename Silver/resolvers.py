# Databricks notebook source
"""
Global resolver + transform registry (shared across ALL platforms).

Two kinds of thing live here:

1. The COLUMN-SPEC resolver -- `build_column(df, target, spec)` -- which turns a
   declarative column spec from the metadata into a single Spark Column. It handles
   the mechanical cases that are identical across every platform:
       {"source": "x", "cast": "double"}                      rename + cast
       {"source": "x"}                                         rename only
       {"source": ["a", "b"], "coalesce": true}               coalesce, first non-null
       {"literal": 0.0, "cast": "double"}                     constant
       {"literal": null, "cast": "string"}                    typed null
   Fixed compose order:  source(s) -> coalesce -> cast -> alias(target)

2. The TRANSFORM registry -- named functions for columns that are genuinely
   COMPUTED, not just renamed/cast. These are PLATFORM-QUALIFIED by name
   (shopify_line_revenue vs cin7_line_revenue) so a platform can never accidentally
   apply another platform's arithmetic. A column spec of {"transform": "name"}
   hands the whole column off to the registered function, which receives the
   flattened DataFrame and returns a Column.

Nothing here is per-client. Clients SELECT which registered transform a column
uses (via an override that names an existing function); they never ship code.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, coalesce, concat_ws, to_timestamp,
    round as spark_round, element_at,
)
from pyspark.sql.column import Column


# ── Transform registry ───────────────────────────────────────────────────────────

TRANSFORMS = {}


def transform(name):
    """Register a named transform. The function takes the flattened DataFrame and
    returns a single Column (already the final value for its target)."""
    def wrap(fn):
        if name in TRANSFORMS:
            raise ValueError(f"Transform '{name}' already registered")
        TRANSFORMS[name] = fn
        return fn
    return wrap


# ── Column-spec resolver ─────────────────────────────────────────────────────────

def build_column(df: DataFrame, target: str, spec: dict) -> Column:
    """Turn one declarative column spec into a Spark Column aliased to `target`.

    Compose order is fixed and total:
        transform  (owns the whole column, short-circuits)     OR
        literal    (constant, optional cast)                   OR
        source(s) -> coalesce (if list) -> cast (if given) -> alias
    """
    # 1. Named transform owns the column entirely. It may take args from the spec
    #    (e.g. {"transform": "concat_ws", "args": {"sep": " ", "cols": ["a","b"]}}),
    #    enabling generic reusable transforms rather than one hardcoded fn per column.
    if "transform" in spec:
        name = spec["transform"]
        if name not in TRANSFORMS:
            # Defensive: the validator should have caught this before Spark ran.
            raise KeyError(
                f"Column '{target}' references unregistered transform '{name}'. "
                f"Known: {sorted(TRANSFORMS)}"
            )
        return TRANSFORMS[name](df, **spec.get("args", {})).alias(target)

    # 2. Literal constant (typed).
    if "literal" in spec:
        c = lit(spec["literal"])
        if spec.get("cast"):
            c = c.cast(spec["cast"])
        return c.alias(target)

    # 3. Source-based: single column or coalesce over a list.
    src = spec["source"]
    if isinstance(src, list):
        c = coalesce(*[col(s) for s in src])
    else:
        c = col(src)

    if spec.get("cast"):
        c = c.cast(spec["cast"])

    return c.alias(target)


def conform(df: DataFrame, columns: dict) -> DataFrame:
    """Apply a whole column map (target -> spec) to a flattened DataFrame."""
    return df.select(*[
        build_column(df, target, spec) for target, spec in columns.items()
    ])


# ── Platform-qualified transforms: SHOPIFY ───────────────────────────────────────
# These reproduce exactly the three computed columns from the Shopify silver
# notebook. line_revenue is GIVEN by Shopify (discountedTotalSet); line_discount_pct
# is derived back out so the line schema matches Cin7; order_line_id is synthesized.

@transform("shopify_line_revenue")
def _shopify_line_revenue(df: DataFrame) -> Column:
    # Net line revenue taken directly from Shopify's discountedTotalSet.
    line_rev = col("discountedTotalSet_shopMoney_amount").cast("double")
    return spark_round(line_rev, 4)


@transform("shopify_line_discount_pct")
def _shopify_line_discount_pct(df: DataFrame) -> Column:
    # Derive discount percent back out so the conformed schema matches Cin7:
    #   gross = unit_price * quantity
    #   pct   = (1 - line_revenue / gross) * 100   (0 when gross is 0/null)
    gross = (
        col("originalUnitPriceSet_shopMoney_amount").cast("double")
        * col("quantity").cast("double")
    )
    line_rev = col("discountedTotalSet_shopMoney_amount").cast("double")
    return spark_round(
        coalesce(
            (lit(1.0) - (line_rev / gross)) * lit(100.0),
            lit(0.0),
        ),
        4,
    )


@transform("shopify_order_line_id")
def _shopify_order_line_id(df: DataFrame) -> Column:
    # No stable line id in the payload -> synthesize deterministically from
    # order_id + sku. (Matches the notebook's concat_ws.)
    return concat_ws("_", col("order_id").cast("string"), col("sku"))


# ── generic parameterized transforms (platform-agnostic) ─────────────────────────
# These take args from the column spec, so one function serves any platform.

@transform("concat_ws")
def _concat_ws(df: DataFrame, sep: str = " ", cols: list = None, cast: str = None) -> Column:
    """Join several source columns with a separator. Cin7 variant = option1 + option2.
    spec: {"transform": "concat_ws", "args": {"sep": " ", "cols": ["option1","option2"]}}"""
    cols = cols or []
    c = concat_ws(sep, *[col(x) for x in cols])
    return c.cast(cast) if cast else c


@transform("primary_association")
def _primary_association(df: DataFrame, path: str = None, index: int = 1,
                         cast: str = "string") -> Column:
    """Pluck one element's id out of a nested association array and carry it up as a
    scalar FK. HubSpot: element_at(associations.companies.results.id, 1).
    spec: {"transform": "primary_association",
           "args": {"path": "associations.companies.results.id", "index": 1}}
    NOTE: this reads a nested array path directly, so it must run on the PRE-flatten
    DataFrame column path. The runner passes the flattened df; for HubSpot the
    association arrays survive flattening (arrays are left untouched), and the dotted
    path resolves against the struct/array nesting. Verified against real bronze."""
    return element_at(col(path), index).cast(cast)


# ── platform-qualified computed transforms: CIN7 ─────────────────────────────────

@transform("cin7_line_revenue")
def _cin7_line_revenue(df: DataFrame) -> Column:
    """Cin7 COMPUTES line revenue: unit_price * qty * (1 - discount_pct/100).
    discount is a LINE-LEVEL PERCENT (confirmed by reconciliation). COALESCE the
    discount to 0 so a null doesn't null the whole line."""
    unit = col("unitPrice").cast("double")
    qty = col("qty").cast("double")
    disc = coalesce(col("discount").cast("double"), lit(0.0))
    return spark_round(unit * qty * (lit(1.0) - (disc / lit(100.0))), 4)


@transform("cin7_order_line_id")
def _cin7_order_line_id(df: DataFrame) -> Column:
    """Synthesize a stable line id from order_id + the line's own id."""
    return concat_ws("_", col("order_id").cast("string"), col("id").cast("string"))
