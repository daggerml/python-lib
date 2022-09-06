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
from time import sleep
import daggerml as dml


# %%
dag = dml.init()


# %%
@dag.func
def doit(n):
    print('running doit with', dag.to_py(n))
    datum = {'sdf': [1, 2, 3, n, ['four', 'five']]}
    sleep(10)  # simulating a function that takes 10 seconds to run
    return datum


# %%
@dag.func
def doit2(x):
    return x['sdf']


# %%
@dag.func
def aaron0(x):
    return x


# %%
@dag.func
def main():
    lst = [doit2(doit(3)), doit(3)] + ([doit(2)] * 10)
    x = aaron0(lst[0])
    return {'list': lst, 'x': x}


# %%
# %%timeit -n 1 -r 1
result = dag.run(main, name='docs/dml-basics').to_py()
print('dag result ==', result)
