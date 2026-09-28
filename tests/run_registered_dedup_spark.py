"""Real local Spark tests; SQL fixtures avoid Windows Python-worker dependency."""
import os
from pathlib import Path
import sys
import unittest
import copy
import jdk4py
import pyspark

os.environ.update(JAVA_HOME=str(jdk4py.JAVA_HOME), SPARK_HOME=str(Path(pyspark.__file__).parent),
                  PYSPARK_PYTHON=sys.executable, SPARK_LOCAL_IP="127.0.0.1")
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
ns = {}
for file in ("gold_resolver_config.py", "gold_engine.py", "gold_strategies.py", "gold_catalog.py"):
    exec(compile((ROOT / "Gold" / file).read_text(encoding="utf-8"), file, "exec"), ns)


def ordered(field="modified"):
    return {"strategy": "ordered", "params": {"order_by": [{"field": field, "direction": "desc"}], "on_tie": "fail"}}


class DedupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("registered-dedup-tests").config("spark.sql.shuffle.partitions", "2").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def run_pipeline(self, sql, steps, identity=None):
        df = self.spark.sql(sql)
        ctx = {"run_id": "test", "processed_at": "2026-09-27"}
        meta = {"_name": "entities", "dedup": {"identity": identity or ["platform", "account", "id"], "strategies": steps, "allow_identity_merge": True}}
        result = ns["run_dedup"](df, meta, ctx)
        return result, ctx

    def versions(self):
        return [{"id": "versions", "strategy": "same_source_identity", "selection": ordered()}]

    def normalized(self, field="name", protected=None):
        return [{"id": "business_key", "strategy": "normalized_key", "params": {
            "scope_by": ["platform", "account"], "keys": [{"field": field, "normalize": "lower_trim"}],
            "must_agree": protected or []}, "selection": ordered()}]

    def test_versions_occurrences_and_account_scope(self):
        result, ctx = self.run_pipeline("SELECT * FROM VALUES ('crm','a','1','old',1),('crm','a','1','new',2),('crm','a','1','new',2),('crm','b','1','other',1) AS t(platform,account,id,name,modified)", self.versions())
        self.assertEqual({r.name for r in result.collect()}, {"new", "other"})
        audit = ctx["dedup_audits"]["entities"].collect()
        self.assertEqual(sum(r.__getitem__("__dd_occurrences") for r in audit), 4)
        self.assertEqual(sum(r.outcome == "excluded" for r in audit), 1)

    def test_versions_tie_fails(self):
        with self.assertRaisesRegex(ValueError, "tied"):
            self.run_pipeline("SELECT * FROM VALUES ('crm','a','1','A',1),('crm','a','1','B',1) AS t(platform,account,id,name,modified)", self.versions())

    def test_missing_identity_fails(self):
        with self.assertRaisesRegex(ValueError, "Missing dedup identity"):
            self.run_pipeline("SELECT 'crm' platform, 'a' account, cast(null as string) id, 1 modified", self.versions())

    def test_companies_conflicting_owner_and_null(self):
        for owner in ("'different'", "cast(null as string)"):
            with self.assertRaisesRegex(ValueError, "protected"):
                self.run_pipeline(f"SELECT * FROM VALUES ('crm','a','1','Gym','owner',1),('crm','a','2',' gym ',{owner},2) AS t(platform,account,id,name,owner,modified)", self.normalized(protected=["owner"]))

    def test_products_blank_sku_is_not_a_match(self):
        result, _ = self.run_pipeline("SELECT * FROM VALUES ('erp','a','1','',1),('erp','a','2','',2),('erp','a','3',cast(null as string),3) AS t(platform,account,id,sku,modified)", self.normalized("sku"))
        self.assertEqual(result.count(), 3)

    def test_contacts_shared_email_conflicting_name(self):
        with self.assertRaisesRegex(ValueError, "protected"):
            self.run_pipeline("SELECT * FROM VALUES ('crm','a','1','family@example.com','Alice',1),('crm','a','2','FAMILY@example.com','Bob',2) AS t(platform,account,id,email,name,modified)", self.normalized("email", ["name"]))

    def external(self):
        return [{"id": "copy", "strategy": "external_reference", "params": {
            "unit_by": ["platform", "account", "order_id"],
            "from": {"where": {"platform": "erp", "account": "a"}, "reference_field": "ref"},
            "to": {"where": {"platform": "shop", "account": "b"}, "identity_field": "order_id"},
            "cardinality": "one_to_one"}, "selection": {"strategy": "referenced_record"}}]

    def test_orders_keep_whole_basket_and_unmatched(self):
        result, ctx = self.run_pipeline("SELECT * FROM VALUES ('erp','a','E','1','S'),('erp','a','E','2','S'),('shop','b','S','a',cast(null as string)),('shop','b','S','b',cast(null as string)),('erp','a','U','3','missing') AS t(platform,account,order_id,line_id,ref)", self.external(), ["platform", "account", "order_id", "line_id"])
        self.assertEqual(result.count(), 3)
        self.assertEqual({r.order_id for r in result.collect()}, {"S", "U"})
        self.assertEqual(ctx["dedup_audits"]["entities"].filter("outcome = 'excluded'").count(), 2)

    def test_partial_integrations_many_to_one_fails(self):
        with self.assertRaisesRegex(ValueError, "one-to-one"):
            self.run_pipeline("SELECT * FROM VALUES ('erp','a','E1','1','S'),('erp','a','E2','2','S'),('shop','b','S','a',cast(null as string)) AS t(platform,account,order_id,line_id,ref)", self.external(), ["platform", "account", "order_id", "line_id"])

    def test_order_lines_conflicting_references_fails(self):
        with self.assertRaisesRegex(ValueError, "Conflicting references"):
            self.run_pipeline("SELECT * FROM VALUES ('erp','a','E','1','S'),('erp','a','E','2',cast(null as string)) AS t(platform,account,order_id,line_id,ref)", self.external(), ["platform", "account", "order_id", "line_id"])

    def test_multiple_steps_and_deterministic_rerun(self):
        sql = "SELECT * FROM VALUES ('crm','a','1','Gym',1),('crm','a','1','Gym',2),('crm','a','2','gym',3) AS t(platform,account,id,name,modified)"
        steps = self.versions() + self.normalized()
        result, ctx = self.run_pipeline(sql, steps)
        self.assertEqual(result.first().id, "2")
        self.assertEqual(ctx["dedup_audits"]["entities"].count(), 5)
        reverse, _ = self.run_pipeline(sql + " ORDER BY modified DESC", steps)
        self.assertEqual(result.collect(), reverse.collect())

    def test_empty_input(self):
        result, _ = self.run_pipeline("SELECT 'crm' platform,'a' account,'1' id,1 modified WHERE false", self.versions())
        self.assertEqual(result.count(), 0)

    def test_alias_composition_and_explicit_resolver_lookup(self):
        first = self.normalized("name")[0]
        second = self.normalized("domain")[0]
        second["id"] = "domain"
        sql = "SELECT * FROM VALUES ('crm','a','1','Gym','old',1),('crm','a','2','gym','new',2),('crm','a','3','Other','new',3) AS t(platform,account,id,name,domain,modified)"
        result, ctx = self.run_pipeline(sql, [first, second])
        self.assertEqual(result.first().id, "3")
        mapping = ctx["dedup_aliases"]["entities"][("platform", "account", "id")]
        self.assertEqual(mapping.count(), 3)
        references = self.spark.sql("SELECT 'crm' platform,'a' account,'1' foreign_id")
        found = ns["lookup_dedup_alias"](references, ctx, "entities", {"platform": "platform", "account": "account", "id": "foreign_id"})
        import json
        self.assertEqual(json.loads(found.first()["__mm_canonical_identity"])["id"], "3")

    def test_product_variant_conflict(self):
        with self.assertRaisesRegex(ValueError, "protected"):
            self.run_pipeline("SELECT * FROM VALUES ('erp','a','1','SKU','red',1),('erp','a','2','sku','blue',2) AS t(platform,account,id,sku,variant,modified)", self.normalized("sku", ["variant"]))

    def test_source_preference_and_strict_config(self):
        steps = self.normalized()
        steps[0]["selection"] = {"strategy": "ordered", "params": {"order_by": [{"field": "origin", "prefer": ["crm", "erp"]}]}}
        result, _ = self.run_pipeline("SELECT * FROM VALUES ('combined','a','1','Gym','erp'),('combined','a','2','gym','crm') AS t(platform,account,id,name,origin)", steps)
        self.assertEqual(result.first().id, "2")
        spec = {"identity": ["id"], "strategies": self.normalized()}
        with self.assertRaisesRegex(ValueError, "allow_identity_merge"):
            ns["validate_dedup_steps"](spec, ["platform", "account", "id", "name", "modified"], ns["DEDUP"], ns["SELECTION"])

    def test_unknown_and_incompatible_strategies(self):
        spec = {"identity": ["id"], "strategies": self.versions()}
        spec["strategies"][0]["selection"] = {"strategy": "referenced_record"}
        with self.assertRaisesRegex(ValueError, "incompatible"):
            ns["validate_dedup_steps"](spec, ["id"], ns["DEDUP"], ns["SELECTION"])
        spec["strategies"][0]["strategy"] = "typo"
        with self.assertRaisesRegex(ValueError, "Unregistered"):
            ns["validate_dedup_steps"](spec, ["id"], ns["DEDUP"], ns["SELECTION"])

    def test_full_engine_keeps_stage_barriers_and_prefixing(self):
        df = self.spark.sql("SELECT * FROM VALUES ('crm','a','1','Gym',1),('crm','a','1','Gym',2) AS t(platform,account,id,name,modified)")
        meta = {"companies": {"entity": "company", "grain_pk": "id", "system_columns": ["platform", "account"],
            "dedup": {"identity": ["platform", "account", "id"], "strategies": self.versions()},
            "resolve": {}, "metrics": [], "enrich": [], "derive": []}}
        ctx = {}
        results = ns["run_gold"](meta, lambda _: df, ctx)
        self.assertEqual(results["companies"].first().company_e_modified, 2)
        self.assertIn("company_e_id", ctx["dims"]["companies"].columns)
        self.assertIn("__mm_dedup_evidence", ctx["surfaces"]["companies"].columns)
        self.assertIn("companies", ctx["dedup_audits"])


if __name__ == "__main__":
    unittest.main()
