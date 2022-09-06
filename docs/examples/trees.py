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
# Lets use some trees
"""

# %%
import os
import daggerml as dml


# %%
dag = dml.init()


# %%
@dag.func
def get_tree_func():
    dkr_build = dag.load('docker')['build']
    tarball = dml.tar(os.path.abspath('./trees/'))
    image = dkr_build(tarball)
    return dml.Func('docker', image)


# %%
@dag.func
def get_data(seed):
    data_gen = dag.load('docs/data-generator')['f']
    return data_gen(seed, 200, 50, 12)


# %%
@dag.func
def main():
    f = get_tree_func()
    data = get_data(20)
    model_params = {'iterations': 10, 'learning_rate': 0.01, 'depth': 4}
    data_params = {'index_cols': None, 'target_col': 'x0'}
    predict_params = {}
    return f('fit', data['train'], model_params, data_params, predict_params)


# %%
resp = dag.run(main, name='docs/trees')
print('dag:', resp)
print('python:', resp.to_py())
