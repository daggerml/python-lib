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
@dml.func
def funkify(image):
    return dml.Func('docker', image)


# %%
@dml.func
def main():
    dkr_build = dml.load('docker')['build']
    tarball = dml.tar(os.path.join(os.path.dirname(dml.__file__),
                                   '../docs/examples/generate-data/'))
    image = dkr_build(tarball)
    f = funkify(image)
    resp = f(20, 200, 50, 12)
    old_resp = dml.load('docs/random-data')
    print(resp)
    print(old_resp)
    return {'f': f, 'data': resp,
            'is-same': resp.force() == old_resp.force()}


# %%
resp = dml.to_py(dml.run(main, name='docs/data-generator'))
print('dag ==', resp)


# %%
import pandas as pd  # noqa: E402
df = pd.read_parquet(resp['data']['train'].data['uri'])
df.head()
