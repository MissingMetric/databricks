import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('dd', Path(__file__).resolve().parents[1] / 'Gold/gold_dedup_config.py')
dd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dd)


class DedupConfigTests(unittest.TestCase):
    def setUp(self):
        self.cols = ['account', 'id', 'name', 'modified', 'owner']
        self.policy = {'identity': ['account', 'id'], 'scope': ['account'],
            'strategies': [{'strategy': 'exact_record'}, {'strategy': 'same_source_identity', 'params': {'order_by': [{'field': 'modified', 'direction': 'desc'}]}},
                {'strategy': 'unique_business_key', 'params': {'keys': [{'field': 'name', 'normalize': 'company_name'}]}}],
            'allow_entity_merge': True, 'conflict_fields': ['owner']}

    def test_valid_and_unknown_fields(self):
        dd.validate_dedup(self.policy, self.cols)
        self.policy['conflict_fields'] = ['missing']
        with self.assertRaises(ValueError):
            dd.validate_dedup(self.policy, self.cols)

    def test_line_merge_requires_explicit_permission(self):
        self.policy['allow_entity_merge'] = False
        with self.assertRaisesRegex(ValueError, 'allow_entity_merge'):
            dd.validate_dedup(self.policy, self.cols)
        self.policy['strategies'][-1]['action'] = 'flag'
        dd.validate_dedup(self.policy, self.cols)

    def test_null_and_unknown_policies_rejected(self):
        for bad in (None, {}, {'strategy': 'keep_first'}):
            with self.assertRaises(ValueError):
                dd.validate_dedup(bad, self.cols)

    def test_scoped_identity_and_strategy_order(self):
        self.policy['identity'] = ['id']
        with self.assertRaises(ValueError):
            dd.validate_dedup(self.policy, self.cols)
        self.policy['identity'] = ['account', 'id']
        self.policy['strategies'].reverse()
        with self.assertRaises(ValueError):
            dd.validate_dedup(self.policy, self.cols)

    def test_alias_cannot_cross_accounts(self):
        self.policy['strategies'][-1] = {'strategy': 'explicit_identity_mapping', 'params': {'aliases': [
            {'from': {'account': 'a', 'id': '1'}, 'to': {'account': 'b', 'id': '2'}}]}}
        with self.assertRaisesRegex(ValueError, 'cross'):
            dd.validate_dedup(self.policy, self.cols)
