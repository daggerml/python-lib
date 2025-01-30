import os

import pytest

from daggerml import Dml


@pytest.fixture(autouse=True)
def add_dml(doctest_namespace):
    with Dml() as dml_:
        for k, v in dml_.kwargs.items():
            os.environ["DML_" + k.upper()] = str(v)
        yield
