#!/usr/bin/env python3
"""
Run this in the test-nevergrad env
"""
from pathlib import Path

import nevergrad as ng

import daggerml as dml
from daggerml.executor import Sh

_here_ = Path(__file__).parent

params = ng.p.Dict(
    num_epochs=ng.p.Scalar(lower=1, upper=5).set_integer_casting(),
    batch_size=ng.p.Scalar(lower=20, upper=55).set_integer_casting(),
    layer0_features=ng.p.Scalar(lower=16, upper=64).set_integer_casting(),
    layer1_features=ng.p.Scalar(lower=32, upper=128).set_integer_casting(),
    learning_rate=ng.p.Scalar(lower=1e-5, upper=0.5),
    momentum=ng.p.Scalar(lower=1e-3, upper=0.99),
)

def mnist_fn(conf):
    from tempfile import TemporaryDirectory

    import mnist
    import numpy as np
    from ml_collections import ConfigDict
    config = ConfigDict(conf)
    with TemporaryDirectory(prefix='mnist-') as tmpd:
        result, loss = mnist.train_and_evaluate(config, tmpd)
    # params_bytes = serialization.to_bytes(result.params)
    return float(np.array(loss))

if __name__ == '__main__':
    params.random_state.seed(12)
    opt = ng.optimizers.NGOpt(parametrization=params, budget=25, num_workers=1)
    with dml.Dag.new('test-dag', 'testing nevergrad with jax in different envs') as dag:
        for _ in range(opt.budget):
            x = opt.ask()
            p = x.value
            node = Sh(dag).run_hatch(mnist_fn, p,
                                     mounts={'mnist.py': _here_/'assets/mnist_jax.py'},
                                     env='test-jax', cache=True)
            loss = dag.get_value(node)
            print('='*80)
            print('loss:', loss)
            print('='*80)
            opt.tell(x, loss)

        recommendation = opt.provide_recommendation()
        p = recommendation.value
        node = Sh(dag).run_hatch(mnist_fn, p,
                                 mounts={'mnist.py': _here_/'assets/mnist_jax.py'},
                                 env='test-jax', cache=True)
        print('='*80)
        print('recommended parameters:', p)
        print('final loss:', dag.get_value(node))
        print('='*80)
        dag.commit({'params': p, 'loss': node})
