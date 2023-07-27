import daggerml as dml
import unittest


class DmlTestBase(unittest.TestCase):

    def tearDown(self):
        # Delete in reverse created_at order to ensure that dags which depend
        # on other dags are deleted before the dags they depend on are deleted.
        # The dml.list_dags() method returns dags orderd by created_at.
        if not (hasattr(self, 'no_delete') and self.no_delete):
            for d in reversed(dml.list_dags()):
                if d['name'].startswith(self.id()):
                    dml.delete_dag(d['id'])
