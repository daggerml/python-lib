#!/usr/bin/env python3
from daggerml.contrib.process import hatch


@hatch(env='test-env')
def hatch_check(dag):
    "hatch functions need to be globally accessible for now"
    import pandas as pd
    return 1  # pd.__version__
