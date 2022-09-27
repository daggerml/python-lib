import os
import json
import pytest
import unittest
import daggerml as dml
from hashlib import md5
from daggerml.exceptions import ApiError


def inc(x):
    return x.to_py() + 1


def div(x, y):
    return x.to_py() / y.to_py()


class TestAll(unittest.TestCase):
    def setUp(self):
        os.environ['DML_LOCAL_DB'] = '1'
        self.dag = dml.init()
        self.inc = self.dag.from_py(inc)
        self.div = self.dag.from_py(div)

    def test_resource(self):
        with pytest.raises(TypeError):
            dml.Resource(1, 2, 3)

    @pytest.mark.filterwarnings("ignore:loading data")
    def test_basic_dag(self):

        @self.dag.from_py
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

        runs = []

        @self.dag.from_py
        def div_n_mutate(p, q):
            runs.append((p.to_py(), q.to_py()))
            return p.to_py() / q.to_py()

        def main():
            return [div_n_mutate(3, 2), div_n_mutate(4, 2), div_n_mutate(4, 2)]

        resp = self.dag.run(main, name='test_local_caching')
        assert runs == [(3, 2), (4, 2)]
        assert resp.to_py() == [3/2, 4/2, 4/2]

    def test_local_redef(self):
        """redefinition doesn't mess with the global thing"""

        @self.dag.from_py
        def div(p, q):
            return q.to_py() / p.to_py()

        def main():
            return div(3, 2)

        resp = self.dag.run(main, name='test_local_redef')
        assert resp.to_py() == 2/3

    def test_local_redef_reuse(self):
        def main():
            return self.div(3, 2)

        resp = self.dag.run(main, name='test_local_redef_reuse')
        assert resp.to_py() == 3/2

    def test_perms(self):
        def main():
            return 1

        resp = self.dag.run(main, name='test_perms')
        resp = json.dumps({'type': 'scalar', 'value': {'type': 'int', 'value': '1'}},
                          sort_keys=True)
        datum_id = md5(resp.encode()).hexdigest()
        x = dml.get_datum(datum_id, gid='test-A')
        assert x['type'] == 'scalar'
        assert x['value']['value'] == '1'
        with pytest.raises(ApiError):
            dml.get_datum(datum_id, gid='test-B')

    def test_list(self):
        @self.dag.from_py
        def gen_list():
            return [0, 1]

        def main():
            lst = gen_list()
            assert len(lst) == 2  # testing __len__
            assert lst[0].to_py() == 0
            assert lst[1].to_py() == 1  # testing __getitem__
            assert [x.to_py() for x in lst] == [0, 1]  # testing __iter__
            return lst

        self.dag.run(main, name='test_list').to_py()

    def test_map(self):
        @self.dag.from_py
        def gen_map():
            return dict(a=2, b=3)

        def main():
            d = gen_map()
            assert len(d) == 2
            assert list(d.keys()) == ['a', 'b']
            assert list(iter(d)) == ['a', 'b']
            assert [x.to_py() for x in d.values()] == [2, 3]
            assert [(k, v.to_py()) for k, v in d.items()] == [('a', 2), ('b', 3)]
            assert d['a'].to_py() == 2
            return d

        self.dag.run(main, name='test_map').to_py()

    def test_resources(self):
        def main():
            return dml.Resource('asdf', 'qwer')
        resp = self.dag.run(main, name='test_resource')
        assert isinstance(resp.to_py(), dml.Resource)
