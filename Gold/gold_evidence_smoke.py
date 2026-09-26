# Databricks notebook source
# Development smoke test: synthetic in-memory data only, no secrets or writes.
# Run this notebook before deploying an evidence-enabled gold build.

# COMMAND ----------
# MAGIC %run ./gold_strategies

# COMMAND ----------
import json
from pyspark.sql.functions import lit

companies = spark.createDataFrame([
    ("c1", "Acme, Inc", "o1", "hubspot"),
    ("c2", "No Owner", None, "order"),
], "company_e_id string, company_e_name string, company_e_owner_id string, source_platform string")
reps = spark.createDataFrame([("o1", "owner@example.test")], "sales_rep_e_id string, sales_rep_e_email string")
orders = spark.createDataFrame([
    ("r1", "Acme Inc.", "direct@example.test", "shopify", 10.0),
    ("r2", "acme", None, "shopify", 20.0),
    ("r3", None, None, "shopify", 30.0),
    ("r4", "acme", None, None, 40.0),
], "orders_e_id string, orders_e_customer_company string, orders_e_sales_rep_email string, source_platform string, amount double")
ctx = {"dims": {"companies": companies, "sales_reps": reps},
       "run_id": "synthetic-smoke", "processed_at": "2026-01-01T00:00:00+00:00"}
meta = {"resolve": {
    "company_e_id": {"strategies": ["company_normalized_name_match", "company_order_string"]},
    "sales_rep_e_email": {"strategies": ["sales_rep_order_native"],
                          "by_platform": {"shopify": {"strategies": ["sales_rep_order_native", "sales_rep_company_owner"]}}},
}}
resolved = run_resolve(orders, meta, ctx)
rows = {r.orders_e_id: r.asDict() for r in resolved.collect()}
assert len(rows) == 4, "Null-platform rows must not disappear"
assert rows["r1"]["sales_rep_e_email"] == "direct@example.test"
assert rows["r2"]["sales_rep_e_email"] == "owner@example.test"
assert rows["r3"]["sales_rep_e_email"] is None
assert rows["r4"]["sales_rep_e_email"] is None
fallback = json.loads(rows["r2"]["sales_rep_e_email_evidence"])
assert fallback["source"] == "company_owner"
assert len(fallback["attempts"]) == 2
assert json.loads(fallback["attempts"][0]["inputs"])["order_rep"] is None
owner_inputs = json.loads(fallback["attempts"][1]["inputs"])
assert owner_inputs["owner_id"] == "o1"
company_trace = json.loads(owner_inputs["company_decision"])
assert json.loads(company_trace["attempts"][0]["inputs"])["matched_company_id"] == "c1"
assert len(json.loads(rows["r1"]["sales_rep_e_email_evidence"])["attempts"]) == 1
unresolved = json.loads(rows["r4"]["sales_rep_e_email_evidence"])
assert unresolved["strategy"] is None and unresolved["status"] == "unresolved"
assert run_resolve(orders.limit(0), meta, ctx).count() == 0

owner = run_resolve(companies, {"resolve": {"sales_rep_e_email": {"strategies": ["owner_id_to_rep_email"]}}}, ctx)
ctx["surfaces"] = {"people": owner}
enriched = run_enrich(resolved, {"enrich": [{"from": "people", "on": "company_e_id",
    "bring": {"sales_rep_e_email": "account_owner"}}]}, ctx)
record = enriched.filter("orders_e_id = 'r2'").first().asDict()
trace = json.loads(record["account_owner_evidence"])
assert json.loads(json.loads(trace["inputs"]["upstream_decision"])["attempts"][0]["inputs"])["owner_id"] == "o1"
assert enriched.count() == orders.count()

metrics = run_metrics(resolved, {"metrics": [{"name": "total", "agg": "sum", "of": "amount", "over": "company_e_id"}]}, ctx)
assert json.loads(metrics.filter("orders_e_id = 'r2'").first().total_evidence)["result"] == 70
derived = run_derive(metrics, {"derive": [{"name": "has_rep", "expr": "is_not_null", "of": "sales_rep_e_email"}]}, ctx)
assert json.loads(derived.filter("orders_e_id = 'r3'").first().has_rep_evidence)["result"] is False
deduped = run_dedup(orders, {"dedup": {"identity": ["source_platform", "orders_e_id"],
    "scope": ["source_platform"], "strategies": [{"strategy": "none"}]}}, ctx)
assert "__mm_dedup_evidence" in deduped.columns
print("Synthetic evidence smoke checks passed. No external data was read or written.")


# Reversing precedence must change the winner where both strategies can resolve.
reverse = {"resolve": {
    "sales_rep_e_email": {"strategies": ["sales_rep_company_owner", "sales_rep_order_native"]},
    "company_e_id": meta["resolve"]["company_e_id"],
}}
reverse_rows = {r.orders_e_id: r.asDict() for r in run_resolve(orders, reverse, ctx).collect()}
assert reverse_rows["r1"]["sales_rep_e_email"] == "owner@example.test"
assert len(json.loads(reverse_rows["r1"]["sales_rep_e_email_evidence"])["attempts"]) == 1

# Null and whitespace native values must fall through, custom target/params work.
blank = orders.limit(1).withColumn("alternate_rep", lit("  "))
custom = run_resolve(blank, {"resolve": {"custom_assignment": {"strategies": [
    {"id": "blank", "strategy": "sales_rep_order_native", "params": {"field": "alternate_rep"}},
    {"id": "native", "strategy": "sales_rep_order_native"}
]}}}, ctx).first().asDict()
assert custom["custom_assignment"] == "direct@example.test"

# Conflicting company matches cannot duplicate rows or silently pick an owner.
ambiguous = companies.unionByName(spark.createDataFrame(
    [("c3", "Acme", "o2", "hubspot")], companies.schema))
try:
    run_resolve(orders, meta, {**ctx, "dims": {**ctx["dims"], "companies": ambiguous}})
except ValueError as exc:
    assert "ambiguous" in str(exc)
else:
    raise AssertionError("Conflicting matches must fail")
print("Composable resolver smoke checks passed.")
