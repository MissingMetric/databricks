"""Synthetic Spark integration tests. Run explicitly; no external data or writes."""
import copy
import json
import os
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
try:
    import jdk4py
    os.environ['JAVA_HOME'] = str(jdk4py.JAVA_HOME)
except ImportError:
    pass
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
import pyspark
os.environ['SPARK_HOME'] = str(Path(pyspark.__file__).parent)
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master('local[2]').appName('dedup-synthetic-tests')
    .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
    .config('spark.ui.showConsoleProgress', 'false')
    .config('spark.driver.bindAddress', '127.0.0.1').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
namespace = {}
for file in ('gold_dedup_config.py', 'gold_dedup.py'):
    exec(compile((ROOT / 'Gold' / file).read_text(encoding='utf-8'), file, 'exec'), namespace)
run = namespace['run_dedup_pipeline']


class DedupSparkTests(unittest.TestCase):
    def setUp(self):
        self.ctx = {'run_id': 'synthetic-test', 'processed_at': '2026-01-01'}
        self.policy = {'identity': ['account', 'id'], 'scope': ['account'], 'allow_entity_merge': True,
            'strategies': [{'strategy': 'exact_record'}, {'strategy': 'same_source_identity', 'params': {'order_by': [{'field': 'modified', 'direction': 'desc'}]}},
                {'strategy': 'unique_business_key', 'params': {'keys': [{'field': 'name', 'normalize': 'company_name'}]}}],
            'conflict_fields': ['owner'], 'on_conflict': 'fail'}

    def frame(self, rows):
        # SQL literals avoid Python worker startup; all values are synthetic.
        schema = 'account string, id string, name string, owner string, modified int'
        if not rows:
            return spark.sql('SELECT CAST(NULL AS STRING) account, CAST(NULL AS STRING) id, CAST(NULL AS STRING) name, CAST(NULL AS STRING) owner, CAST(NULL AS INT) modified WHERE false')
        def sql(v):
            if v is None: return 'NULL'
            if isinstance(v, int): return str(v)
            return "'" + v.replace("'", "''") + "'"
        values = ','.join('(' + ','.join(sql(v) for v in row) + ')' for row in rows)
        return spark.sql('SELECT * FROM VALUES ' + values + ' AS t(account,id,name,owner,modified)')

    def execute(self, rows):
        return run(self.frame(rows), {'_name': 'companies', 'dedup': self.policy}, self.ctx)

    def tearDown(self):
        spark.catalog.clearCache()

    def test_iron_house_alias_and_audit(self):
        rows = [('hubspot','316522121920','Iron House Gym','163339292',1), ('hubspot','317966651113','Iron House Gym','163339292',1)]
        out = self.execute(rows).collect()
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].id, '316522121920')
        aliases = self.ctx['dedup_aliases']['companies'].collect()
        self.assertEqual(len(aliases), 2)
        audit = self.ctx['dedup_audits']['companies']['records'].collect()
        self.assertEqual({r.decision for r in audit}, {'selected', 'consolidated_alias'})
        self.assertTrue(all(r.source_record for r in audit))
        self.assertEqual(json.loads(out[0]['__mm_dedup_evidence'])['strategy'], 'dedup.pipeline@1')

    def test_versions_exact_duplicates_and_account_scope(self):
        rows = [('a','1','Gym','rep',1),('a','1','Gym','rep',2),('a','1','Gym','rep',2),('b','1','Gym','rep',1)]
        out = self.execute(rows).collect()
        self.assertEqual(len(out), 2)
        self.assertEqual(next(r.modified for r in out if r.account == 'a'), 2)
        audit = self.ctx['dedup_audits']['companies']['records'].collect()
        self.assertEqual(sum(r.occurrences for r in audit), 4)

    def test_conflict_fail_and_retain(self):
        rows = [('a','1','Gym','rep1',1),('a','2','Gym','rep2',1)]
        with self.assertRaisesRegex(ValueError, 'protected'):
            self.execute(rows)
        self.policy['on_conflict'] = 'retain'
        self.assertEqual(self.execute(rows).count(), 2)
        self.assertEqual(self.ctx['dedup_audits']['companies']['conflicts'].count(), 2)

    def test_tied_revisions_and_missing_identity_fail(self):
        for rows in ([('a','1','Gym','rep1',1),('a','1','Gym','rep2',1)], [('a',None,'Gym','rep',1)]):
            with self.assertRaises(ValueError):
                self.execute(rows)

    def test_null_keys_and_flag_only_never_merge(self):
        self.policy['strategies'][-1]['action'] = 'flag'
        rows = [('a','1','Gym','rep',1),('a','2','Gym','rep',1),('a','3',None,'rep',1),('a','4',None,'rep',1)]
        self.assertEqual(self.execute(rows).count(), 4)
        self.assertEqual(self.ctx['dedup_audits']['companies']['matches'].count(), 1)

    def test_explicit_mapping_selects_requested_id_and_rewrites_foreign_key(self):
        self.policy['strategies'][-1] = {'strategy': 'explicit_identity_mapping', 'params': {'aliases': [
            {'from': {'account':'a','id':'1'}, 'to': {'account':'a','id':'2'}}]}}
        self.assertEqual(self.execute([('a','1','Gym','rep',1),('a','2','Other','rep',1)]).first().id, '2')
        source = spark.sql("SELECT 'a' account, '1' company_id")
        result = namespace['apply_identity_links'](source, {'identity_links': [{'field':'company_id','table':'companies','key':'id','scope':{'account':'account'}}]}, {'companies': {'dedup': self.policy}}, self.ctx)
        self.assertEqual(result.first().company_id, '2')

    def test_order_lines_exact_only_keep_separate_ids_and_empty_input(self):
        self.policy['strategies'] = [{'strategy':'exact_record'}]
        self.policy['allow_entity_merge'] = False
        rows = [('a','line1','SKU','rep',1),('a','line2','SKU','rep',1),('a','line1','SKU','rep',1)]
        self.assertEqual(self.execute(rows).count(), 2)
        self.assertEqual(self.execute([]).count(), 0)

    def test_multiple_strategies_cannot_hide_conflicting_groups(self):
        self.policy['strategies'].append({'id':'by_owner', 'strategy':'unique_business_key', 'params': {'keys':[{'field':'owner'}]}})
        rows = [('a','1','Gym','rep1',1),('a','2','Gym','rep2',1),('a','3','Other','rep2',1)]
        self.policy['on_conflict'] = 'retain'
        self.assertEqual(self.execute(rows).count(), 3)

    def test_full_engine_dedup_before_company_resolution(self):
        for file in ('gold_resolver_config.py', 'gold_engine.py', 'gold_strategies.py'):
            exec(compile((ROOT / 'Gold' / file).read_text(encoding='utf-8'), file, 'exec'), namespace)
        company_policy = copy.deepcopy(self.policy)
        company_policy['identity'] = ['source_platform', 'id']
        company_policy['scope'] = ['source_platform']
        company_policy['conflict_fields'] = ['owner_id']
        company_policy['strategies'][1]['params']['order_by'] = [{'field':'modified_date','direction':'desc'}]
        companies = spark.sql("SELECT * FROM VALUES ('hubspot','316522121920','Iron House Gym','163339292',1), ('hubspot','317966651113','Iron House Gym','163339292',1) AS t(source_platform,id,name,owner_id,modified_date)")
        reps = spark.sql("SELECT 'hubspot' source_platform, '163339292' id, 'rep@example.test' email")
        orders = spark.sql("SELECT 'store' source_platform, 'line1' id, 'Iron House Gym' customer_company, CAST(NULL AS STRING) sales_rep_email")
        basic = {'identity':['source_platform','id'], 'scope':['source_platform'], 'strategies':[{'strategy':'exact_record'}]}
        meta = {
            'companies': {'entity':'company', 'grain_pk':'id', 'dedup':company_policy},
            'sales_reps': {'entity':'sales_rep', 'grain_pk':'id', 'dedup':basic},
            'orders': {'entity':'orders', 'grain_pk':'id', 'dedup':basic, 'resolve': {
                'company_e_id': {'strategies':['company_normalized_name_match']},
                'sales_rep_e_email': {'strategies':['sales_rep_order_native','sales_rep_company_owner']}}},
        }
        data = {'company':companies, 'sales_rep':reps, 'orders':orders}
        result = namespace['run_gold'](meta, lambda tm: data[tm['entity']], self.ctx)
        row = result['orders'].first()
        self.assertEqual(row.company_e_id, '316522121920')
        self.assertEqual(row.sales_rep_e_email, 'rep@example.test')
        inputs = json.loads(json.loads(row.company_e_id_evidence)['attempts'][0]['inputs'])
        self.assertEqual(json.loads(inputs['company_dedup_decision'])['strategy'], 'dedup.pipeline@1')

    def test_external_reference_partial_sync_precedence_and_cardinality(self):
        self.policy['strategies'][-1] = {'strategy':'external_reference', 'params': {
            'reference_field':'name', 'target_field':'id', 'from_scope':{'account':'erp'}, 'to_scope':{'account':'store'}}}
        self.policy['selection'] = {'prefer_values':[{'field':'account','values':['store','erp']}]}
        rows = [('erp','e1','s1','rep',1),('store','s1','basket','rep',1),('erp','e2','missing','rep',1)]
        result = self.execute(rows).collect()
        self.assertEqual({r.id for r in result}, {'s1','e2'})
        with self.assertRaisesRegex(ValueError, 'one-to-one'):
            self.execute(rows + [('erp','e3','s1','rep',1)])

    def test_rerun_and_row_order_produce_same_aliases(self):
        rows = [('a','2','Gym','rep',1),('a','1','Gym','rep',1)]
        first = self.execute(rows).first().id
        second = self.execute(list(reversed(rows))).first().id
        self.assertEqual(first, second)


if __name__ == '__main__':
    try:
        program = unittest.main(exit=False)
    finally:
        spark.stop()
    sys.exit(0 if program.result.wasSuccessful() else 1)
