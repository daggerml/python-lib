import os
import pytest
import unittest
import daggerml as dml


def inc(x):
    return x.to_py() + 1


def div(x, y):
    return x.to_py() / y.to_py()


class TestAll(unittest.TestCase):
    def setUp(self):
        os.environ['DML_LOCAL_DB'] = '1'
        self.dag = dml.init()
        self.inc = self.dag.func(inc)
        self.div = self.dag.func(div)

    def test_resource0(self):
        with self.assertRaises(TypeError):
            dml.Resource(1, 2, 3)

    def test_resource(self):
        with pytest.raises(TypeError):
            dml.Resource(1, 2, 3)

    @pytest.mark.filterwarnings("ignore:loading data")
    def test_basic_dag(self):

        @self.dag.func
        def get_list_of_maps(base, n):
            base, n = base.to_py(), n.to_py()
            return [{str(y): self.inc(base + y)} for y in range(n)]

        def main(base, n):
            lom = get_list_of_maps(base, n)
            return lom, lom[0], lom[0]['0']

        resp = self.dag.run(main, 2, 3, name='test_basic_dag').to_py()
        real = [{str(y): 2 + y + 1} for y in range(3)]
        assert resp == self.dag.load('test_basic_dag')
        assert resp == [real, real[0], real[0]['0']]

    def test_raises(self):

        with pytest.raises(dml.NodeError):
            self.dag.run(self.div, 2, 0, name='test_raises').to_py()

    @pytest.mark.filterwarnings("ignore:loading data")
    def test_try_except(self):

        def main_try_except(base, n):
            base, n = base.to_py(), n.to_py()
            resp = []
            for i in range(n):
                try:
                    y = self.div(base, i)
                except dml.NodeError:
                    y = None
                resp.append(y)
            return resp
        resp = self.dag.run(main_try_except, 2, 3, name='test_try_except').to_py()
        assert resp == self.dag.load('test_try_except')
        assert resp == [None] + [2 / i for i in range(1, 3)]

    def test_local_caching(self):

        n_iters = 20
        runs = []

        @self.dag.func
        def div_n_mutate(p, q):
            runs.append(p.to_py())
            return p.to_py() / q.to_py()

        def main():
            return [div_n_mutate(3, 2) for _ in range(n_iters)] + [div_n_mutate(5, 2)]

        resp = self.dag.run(main, name='test_local_caching')
        assert len(runs) == 2
        assert resp.to_py() == [3/2 for _ in range(n_iters)] + [5/2]

    def test_local_redef(self):
        """redefinition doesn't do anything"""

        n_iters = 20

        @self.dag.func
        def div(p, q):
            return q.to_py() / p.to_py()

        def main():
            return [div(3, 2) for _ in range(n_iters)]

        resp = self.dag.run(main, name='test_local_redef')
        assert resp.to_py() == [2/3 for _ in range(n_iters)]

    def test_local_redef_reuse(self):
        """redefinition doesn't do anything"""

        n_iters = 20

        def main():
            return [self.div(3, 2) for _ in range(n_iters)]

        resp = self.dag.run(main, name='test_local_redef_reuse')
        assert resp.to_py() == [3/2 for _ in range(n_iters)]
