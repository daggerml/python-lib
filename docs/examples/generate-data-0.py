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
# Docker Func Basics
"""

# %%
import os
import daggerml as dml


# %%
@dml.func
def funkify(image):
    return dml.Func('docker', image)


# %%
@dml.func
def main():
    dkr_build = dml.load('docker')['build']
    tarball = dml.tar(os.path.abspath('./generate-data/'))
    image = dkr_build(tarball)
    f = funkify(image)
    return f(20, 200, 50, 12)


# %%
resp = dml.to_py(dml.run(main, name='docs/random-data'))
print('dag ==', resp)


# %%
import pandas as pd  # noqa: E402
df = pd.read_parquet(resp['train'].data['uri'])
df.head()
