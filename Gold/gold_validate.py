# Databricks notebook source
# Gold metadata validator
# ─────────────────────────────────────────────────────────────────────────────
# Runs BEFORE any Spark work, against the gold table metadata + the registries.
# Two-pass because enrichment references OTHER tables' surfaces:
#
#   Pass 1 -- compute each table's declared PUBLIC SURFACE (the column names it
#             will produce through stages 1-3: base columns + resolved + metrics).
#   Pass 2 -- validate every table against those surfaces:
#             * every strategy named (dedup/resolve/metric/enrich/derive) registered
#             * every metric's `over` is an IDENTITY column (own key or a resolved
#               FK) -- produced by stage 1 or 2, never a measure or enrichment
#             * every enrich `from` table exists, and every `bring` source column
#               is in that table's public surface (never its enrichments)
#
# The surface is computed from metadata (declared base columns + resolved targets
# + metric names), so validation needs no data -- it runs in CI over the JSON.

# COMMAND ----------

# MAGIC %run ./gold_resolver_config

# COMMAND ----------

class GoldMetaError(Exception):
    pass


def _prefix_base(tmeta):
    """Apply the engine's native-prefixing rules to the declared bare base columns,
    so the validator's surface matches what the engine actually produces:
      grain_pk -> <entity>_e_id ; carried_keys -> <entity>_e_<key> ;
      system columns -> unchanged ; else -> <entity>_e_<bare>.
    Mirrors prefix_native() in the engine.
    """
    entity = tmeta.get("entity", tmeta.get("_name"))
    system = set(tmeta.get("system_columns", ["source_platform"]))
    grain_pk = tmeta.get("grain_pk")
    carried = tmeta.get("source", {}).get("carried_keys", {})
    out = []
    for c in tmeta.get("base_columns", []):
        if c in system or "_e_" in c:
            out.append(c)
        elif c == grain_pk:
            out.append(f"{entity}_e_id")
        elif c in carried:
            out.append(f"{entity}_e_{carried[c]}")
        else:
            out.append(f"{entity}_e_{c}")
    return out


def _resolved_targets(tmeta):
    """Identity/measure columns a table's stage-2 resolve produces (the dict keys).
    These are declared in metadata already in their final (prefixed) form, e.g.
    'company_e_id', 'sales_rep_e_email' -- the resolver's canonical output name."""
    return list(tmeta.get("resolve", {}).keys())


def _metric_names(tmeta):
    return [m["name"] for m in tmeta.get("metrics", [])]


def _base_columns(tmeta):
    """Columns present on the table's base frame BEFORE stages -- declared in
    metadata as `base_columns` (BARE silver names). Prefixed here to match the
    engine's output naming."""
    return _prefix_base(tmeta)


def compute_meta_surface(tmeta):
    """Public surface = base + resolved + metrics (stages 1-3). NOT enrich/derive."""
    return set(_base_columns(tmeta)) | set(_resolved_targets(tmeta)) | set(_metric_names(tmeta))


def identity_columns(tmeta):
    """Columns valid as a metric `over`: the table's own key (grain_pk -> _e_id,
    or prefixed own_keys) plus every resolved FK (already prefixed in metadata)."""
    entity = tmeta.get("entity", tmeta.get("_name"))
    grain_pk = tmeta.get("grain_pk")
    carried = tmeta.get("source", {}).get("carried_keys", {})
    ids = set(_resolved_targets(tmeta))
    for k in tmeta.get("own_keys", []):
        if k == grain_pk:
            ids.add(f"{entity}_e_id")
        elif k in carried:
            ids.add(f"{entity}_e_{carried[k]}")
        else:
            ids.add(f"{entity}_e_{k}")
    return ids


def validate_gold(tables_meta, DEDUP, RESOLVE, METRIC, ENRICH, DERIVE):
    errors = []

    # Pass 1: surfaces
    surfaces = {name: compute_meta_surface(tm) for name, tm in tables_meta.items()}

    # Pass 2: per-table checks
    for name, tm in tables_meta.items():

        # dedup strategy registered
        d = tm.get("dedup", {})
        if d and d.get("strategy") and d["strategy"] not in DEDUP:
            errors.append(f"[{name}] dedup strategy '{d['strategy']}' not registered")

        try:
            resolver_order(tm.get("resolve", {}), RESOLVE,
                _base_columns({**tm, "_name": name}))
        except ValueError as exc:
            errors.append(f"[{name}] {exc}")

        # metrics: strategy registered + `over` is an identity column
        ids = identity_columns(tm)
        for m in tm.get("metrics", []):
            if m["agg"] not in METRIC:
                errors.append(f"[{name}] metric '{m['name']}' agg '{m['agg']}' not registered")
            over = m.get("over")
            if over is not None and over not in ids:
                errors.append(
                    f"[{name}] metric '{m['name']}' groups over '{over}', which is not an "
                    f"identity column (own_keys or resolved FK). Identities: {sorted(ids)}. "
                    f"Metrics may only group over keys, never measures or enrichments."
                )

        # enrich: from-table exists, strategy registered, bring cols in source surface
        for e in tm.get("enrich", []):
            frm = e.get("from")
            if frm not in tables_meta:
                errors.append(f"[{name}] enrich references unknown table '{frm}'")
                continue
            estrat = e.get("strategy", "left_join_bring")
            if estrat not in ENRICH:
                errors.append(f"[{name}] enrich from '{frm}' strategy '{estrat}' not registered")
            src_surface = surfaces[frm]
            for scol in (e.get("bring") or {}):
                if scol not in src_surface:
                    errors.append(
                        f"[{name}] enrich from '{frm}' brings '{scol}', which is not in "
                        f"'{frm}' public surface {sorted(src_surface)}. Enrichment may only "
                        f"pull native+resolved+metric columns, never another table's enrichments."
                    )

        # derive: strategy registered (derive may reference any row column, so no
        # surface check -- it's terminal and consumed by nobody)
        for dv in tm.get("derive", []):
            if dv["expr"] not in DERIVE:
                errors.append(f"[{name}] derive '{dv['name']}' expr '{dv['expr']}' not registered")

    if errors:
        raise GoldMetaError(
            f"{len(errors)} gold metadata error(s):\n" + "\n".join(f"  - {e}" for e in errors)
        )
    return surfaces

# COMMAND ----------

print("Gold validator loaded.")
