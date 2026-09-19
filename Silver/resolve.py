# Databricks notebook source
# Resolution layer -- the ONLY module that reads config sources (ADLS or Supabase).
#
# resolve(spark, slug, platform, storage_account) does, in order:
#   1. load the BASE for the platform from the shared ADLS `configs` container
#      (silver mapping + ingest + common model)
#   2. fetch the client's row from Supabase client_platforms (active, entities, overrides)
#   3. deep_merge(base, overrides)
#   4. filter to the client's enabled entities (+ the reconciliations that survive)
#   5. validate the resolved config -- raises on any problem
#   6. return one self-contained, validated config dict
#
# Everything downstream (silver_runner) receives that dict and does NO further
# I/O. Resolve once, validate once, pass it down.
#
# ADLS layout expected in the `configs` container:
#   Common/common_model.json
#   Platforms/<platform>/<platform>_silver.json
#   Platforms/<platform>/<platform>_ingest.json

# COMMAND ----------

import copy
import json

# COMMAND ----------

def download_json(spark, path: str) -> dict:
    """Read a whole JSON file from ADLS as one dict. `wholetext` keeps the file
    intact (no per-line record parsing), then json.loads the concatenated text."""
    rows = spark.read.option("wholetext", "true").text(path).collect()
    return json.loads("".join(r.value for r in rows))


def config_root(storage_account: str, container: str = "configs") -> str:
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net"

# COMMAND ----------

# ── Supabase fetch (isolated) ────────────────────────────────────────────────────

def fetch_client_platform(slug: str, platform: str) -> dict:
    """Fetch one client_platforms row via the slug-resolution view.

    Expected row shape:
        { "active": bool,
          "entities": [str] | None,   # None = all base entities enabled
          "overrides": dict | None }  # None = pure defaults

    Uses the SECRET key (sb_secret_...) -- client_platforms is RLS-locked and the
    secret key bypasses RLS. The publishable key is RLS-subject and would read
    ZERO rows (a confusing "no row found" rather than an auth error).
    """
    from supabase import create_client

    url = dbutils.secrets.get(scope="kv", key="SUPABASE-URL")
    key = dbutils.secrets.get(scope="kv", key="SUPABASE-SECRET")
    sb = create_client(url, key)

    resp = (
        sb.table("client_platforms_by_slug")
        .select("active, entities, overrides")
        .eq("client_slug", slug)
        .eq("platform", platform)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        raise LookupError(
            f"No client_platforms row for slug='{slug}', platform='{platform}'. "
            f"Insert one, or check the slug exists in clients."
        )
    return rows[0]

# COMMAND ----------

# ── merge + filter ───────────────────────────────────────────────────────────────

def deep_merge(base: dict, override: dict, _spec_level: bool = False) -> dict:
    """Recursively merge override INTO a copy of base.

    CRITICAL: a column SPEC is a leaf. The merge recurses through the structure and
    through the `columns` dict (so an override can add/replace individual columns),
    but it does NOT recurse INTO a column spec. Overriding one column replaces its
    whole spec -- never half-merge {"literal": null} with {"source": ...} and end
    up with a spec carrying both (build_column would silently pick one and ignore
    the other)."""
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        both_dicts = k in out and isinstance(out[k], dict) and isinstance(v, dict)
        if both_dicts and not _spec_level:
            out[k] = deep_merge(out[k], v, _spec_level=(k == "columns"))
        else:
            out[k] = copy.deepcopy(v)
    return out


def filter_enabled(config: dict, entities) -> dict:
    """Keep only enabled entities. entities=None means 'all base entities'. Also
    drops any reconciliation whose parent or child is no longer enabled, so turning
    an entity off never leaves a dangling recon."""
    out = copy.deepcopy(config)
    all_entities = out.get("entities", {})

    if entities is None:
        enabled = set(all_entities)
    else:
        enabled = set(entities)
        unknown = enabled - set(all_entities)
        if unknown:
            raise ValueError(
                f"client enabled entities not defined in base: {sorted(unknown)}"
            )

    out["entities"] = {n: e for n, e in all_entities.items() if n in enabled}
    out["reconciliations"] = [
        r for r in out.get("reconciliations", [])
        if r.get("parent") in enabled and r.get("child") in enabled
    ]
    return out

# COMMAND ----------

# ── entry point ──────────────────────────────────────────────────────────────────

def resolve(spark, slug: str, platform: str, storage_account: str,
            row: dict = None) -> dict:
    """Produce a resolved, validated, self-contained config for one client+platform.

    Pass `row` explicitly to bypass Supabase (tests / local runs); otherwise it is
    fetched from client_platforms.
    """
    root = config_root(storage_account)
    base = download_json(spark, f"{root}/Platforms/{platform}/{platform}_silver.json")
    ingest = download_json(spark, f"{root}/Platforms/{platform}/{platform}_ingest.json")
    common_model = download_json(spark, f"{root}/Common/common_model.json")

    if row is None:
        row = fetch_client_platform(slug, platform)

    if not row.get("active", False):
        raise ValueError(f"platform '{platform}' is not active for client '{slug}'")

    merged = deep_merge(base, row.get("overrides") or {})
    config = filter_enabled(merged, row.get("entities"))

    # Stamp identity so the pipeline is fully self-contained (no reach-back).
    config["slug"] = slug
    config["platform"] = platform

    # Validate the EXACT dict the pipeline will run.
    validate(config, common_model, ingest, TRANSFORMS.keys())

    return config