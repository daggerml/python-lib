# ---
# jupyter:
#   jupytext:
#     cell_markers: '"""'
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.9.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
"""
# DaggerML Basics

First, set up the environment:
"""

# %%
# import os
from time import sleep
from daggerml import func, to_py, run
# os.environ['DML_LOCAL_DB'] = '1'


# %%
@func
def doit(n):
    print('running doit with', to_py(n))
    datum = {'sdf': [1, 2, 3, n, ['four', 'five']]}
    sleep(10)
    return datum


# %%
@func
def doit2(x):
    return x['sdf']


# %%
@func
def aaron0(x):
    return x


# %%
@func
def main():
    lst = [doit2(doit(3)), doit(3), doit(2), doit(2), doit(2)]
    x = aaron0(lst[0])
    # This code below will not be run because it's not returned.
    [doit(x) for x in range(5, 10)]
    return {'list': lst, 'x': x}


# %%
# %%timeit -n 1 -r 1
result = to_py(run(main, name='docs/dml-basics'))
print('dag result ==', result)
