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
dag = dml.init()


# %%
@dag.func
def funkify(image):
    return dml.Func('docker', image)


# %%
@dag.func
def main():
    dkr_build = dag.load('docker')['build']
    tarball = dml.tar(os.path.join(os.path.dirname(dml.__file__),
                                   '../docs/examples/generate-data/'))
    image = dkr_build(tarball)
    f = funkify(image)
    return f(20, 200, 50, 12)


# %% [markdown]
"""
Normally we'd be able to pass in relative paths into `dml.tar`, but
because of the way we run our documentation, it's just easier to pass in
absolute paths (the question of "relative to what" is binding).
"""


# %%
resp = dag.run(main, name='docs/random-data').to_py()
print('dag ==', resp)


# %%
import pandas as pd  # noqa: E402
df = pd.read_parquet(resp['train'].data['uri'])
df.head()
