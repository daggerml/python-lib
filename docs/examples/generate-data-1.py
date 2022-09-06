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
# Structural Sharing Across Dags
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
    resp = f(20, 200, 50, 12)
    old_resp = dag.load('docs/random-data')
    print(resp)
    print(old_resp)
    return {'f': f, 'data': resp,
            'is-same': resp.to_py() == old_resp.to_py()}


# %%
resp = dag.run(main, name='docs/data-generator').to_py()
print('dag ==', resp)


# %%
import pandas as pd  # noqa: E402
df = pd.read_parquet(resp['data']['train'].data['uri'])
df.head()
