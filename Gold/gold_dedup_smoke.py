# Databricks notebook source
# Synthetic development test. No cloud data, secrets or output writes.
# COMMAND ----------
# MAGIC %run ./gold_strategies
# COMMAND ----------
import json

sample = spark.sql("""
SELECT * FROM VALUES
 ('hubspot', '316522121920', 'Iron House Gym', '163339292', 1),
 ('hubspot', '317966651113', 'Iron House Gym', '163339292', 1),
 ('hubspot', '317966651113', 'Iron House Gym', '163339292', 1)
AS companies(source_platform, id, name, owner_id, modified_date)
""")
policy = {
    "identity": ["source_platform", "id"], "scope": ["source_platform"],
    "allow_entity_merge": True,
    "strategies": [
        {"strategy": "exact_record"},
        {"strategy": "same_source_identity", "params": {"order_by": [{"field": "modified_date", "direction": "desc"}]}},
        {"strategy": "unique_business_key", "params": {"keys": [{"field": "name", "normalize": "company_name"}]}}
    ],
    "conflict_fields": ["owner_id"], "on_conflict": "fail",
    "selection": {"order_by": []}
}
smoke_ctx = {"run_id": "dedup-smoke", "processed_at": "2026-01-01"}
result = run_dedup(sample, {"_name": "companies", "dedup": policy}, smoke_ctx)
assert result.count() == 1
assert result.first().id == "316522121920"
assert smoke_ctx["dedup_aliases"]["companies"].count() == 2
records = smoke_ctx["dedup_audits"]["companies"]["records"]
assert records.agg({"occurrences": "sum"}).first()[0] == 3
assert {r.decision for r in records.collect()} == {"selected", "consolidated_alias"}
assert json.loads(result.first()["__mm_dedup_evidence"])["strategy"] == "dedup.pipeline@1"
display(result)
display(smoke_ctx["dedup_aliases"]["companies"])
display(records)
print("Dedup smoke test passed. No external data was read or written.")
