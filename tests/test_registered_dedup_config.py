"""Registry-backed configuration checks, without starting Spark."""
from pathlib import Path
import unittest
import copy

ROOT = Path(__file__).resolve().parents[1]
ns = {}
for filename in ("gold_resolver_config.py", "gold_engine.py", "gold_strategies.py"):
    exec(compile((ROOT / "Gold" / filename).read_text(encoding="utf-8"), filename, "exec"), ns)


class DedupConfigTests(unittest.TestCase):
    def setUp(self):
        self.columns = ["platform", "account", "id", "modified", "name"]
        self.spec = {"identity": ["platform", "account", "id"], "strategies": [
            {"id": "latest", "strategy": "same_source_identity", "selection": {
                "strategy": "ordered", "params": {"order_by": [{"field": "modified", "direction": "desc"}]}}}]}

    def validate(self, spec=None):
        ns["validate_dedup_steps"](spec or self.spec, self.columns, ns["DEDUP"], ns["SELECTION"])

    def test_valid_registry_dispatch(self):
        self.validate()

    def test_rejects_bad_identity_and_unknown_options(self):
        for value in ([], ["unknown"], [["id"]], ["id", "id"]):
            spec = copy.deepcopy(self.spec)
            spec["identity"] = value
            with self.assertRaises(ValueError):
                self.validate(spec)
        self.spec["unexpected"] = True
        with self.assertRaises(ValueError):
            self.validate()

    def test_rejects_duplicate_steps_and_unknown_parameters(self):
        self.spec["strategies"] *= 2
        with self.assertRaises(ValueError):
            self.validate()
        self.spec["strategies"] = self.spec["strategies"][:1]
        self.spec["strategies"][0]["params"] = {"unknown": "id"}
        with self.assertRaises(ValueError):
            self.validate()

    def test_rejects_unsupported_preference_fallback(self):
        self.spec["strategies"][0]["selection"]["params"]["order_by"] = [
            {"field": "platform", "prefer": ["crm"], "unlisted": "first"}]
        with self.assertRaises(ValueError):
            self.validate()

    def test_client_section_is_replaced_not_merged(self):
        old = {"t": {"dedup": {"strategy": "keep_first", "on": "old"}}}
        result = ns["client_resolver_tables"](old, {"schema_version": 1, "tables": {"t": {"resolve": {}, "dedup": self.spec}}})
        self.assertEqual(result["t"]["dedup"], self.spec)
        self.assertEqual(old["t"]["dedup"]["on"], "old")

    def test_normalization_and_cross_id_opt_in(self):
        self.spec["strategies"][0].update(strategy="normalized_key", params={
            "scope_by": ["platform", "account"], "keys": [{"field": "name"}]})
        with self.assertRaisesRegex(ValueError, "allow_identity_merge"):
            self.validate()
        self.spec["allow_identity_merge"] = True
        self.validate()
        self.spec["strategies"][0]["params"]["must_agree"] = "name"
        with self.assertRaises(ValueError):
            self.validate()


if __name__ == "__main__":
    unittest.main()
