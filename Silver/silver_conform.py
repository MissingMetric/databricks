# Databricks notebook source
# Silver: metadata-driven conform (ALL platforms)
# ─────────────────────────────────────────────────────────────────────────────
# This single notebook replaces the per-platform silver notebooks
# (shopify_silver, cin7_silver, hubspot_silver). Platform-specific knowledge
# lives in metadata, not code:
#
#   configs/Platforms/<platform>/<platform>_silver.json   conform mapping
#   configs/Platforms/<platform>/<platform>_ingest.json   bronze field contract
#   configs/Common/common_model.json                      the target contract
#   Supabase client_platforms                             enablement + overrides
#
# Flow:  resolve(...) -> loads base + client overrides, validates -> run(...)
#
# Parameters: slug (client container), platform (shopify | cin7 | hubspot)

# COMMAND ----------

# MAGIC %run ./resolvers

# COMMAND ----------

# MAGIC %run ./validate

# COMMAND ----------

# MAGIC %run ./resolve

# COMMAND ----------

# MAGIC %run ./silver_runner

# COMMAND ----------

# ── Parameters ──────────────────────────────────────────────────────────────────
dbutils.widgets.text("slug", "development", "Client Slug")
dbutils.widgets.text("platform", "hubspot", "Platform (shopify|cin7|hubspot)")

slug = dbutils.widgets.get("slug")
platform = dbutils.widgets.get("platform")

if not slug:
    raise ValueError("slug parameter is required")
if not platform:
    raise ValueError("platform parameter is required")

storage_account = dbutils.secrets.get(scope="kv", key="adls-storage-account")

print(f"Running {platform} silver for: {slug}")

# COMMAND ----------

# # ── External location (self-provision for this slug if it doesn't exist) ─────────
# location_name = f"loc_{slug.replace('-', '_')}"

# spark.sql(f"""
#     CREATE EXTERNAL LOCATION IF NOT EXISTS `{location_name}`
#     URL 'abfss://{slug}@{storage_account}.dfs.core.windows.net/'
#     WITH (STORAGE CREDENTIAL `databricksmanagedidentity`)
# """)

# print(f"✓ External location ready for {slug}")

# COMMAND ----------

# ── RESOLVE: base config + client overrides, validated before any Spark work ─────
# Raises with the full list of problems if anything is wrong (unknown transform,
# schema drift from the common model, a conform source bronze doesn't pull, a
# missing merge key, a dangling dependency).

config = resolve(spark, slug, platform, storage_account)

print(f"Resolved config: entities = {list(config['entities'])}")
for name, ent in config["entities"].items():
    print(f"  {name:20} table={ent['table']:12} shape={ent['shape']}")

# COMMAND ----------

# ── RUN: extract -> conform -> reconcile -> write -> merge ───────────────────────

conformed = run(spark, config, storage_account)

# COMMAND ----------

print(f"""
{platform} silver complete for [{slug}]
──────────────────────────────────────────────
  entities: {list(config['entities'])}
  written to silver/platforms/{platform}/<entity>/
  merged into silver/combined/<entity>_combined/
""")