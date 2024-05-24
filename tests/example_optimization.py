#!/usr/bin/env python3
"""
without any hatch environments loaded, run:
`hatch -e test-nevergrad run {{this_file}}`

You can see the effect of caching by increasing the budget parameter.

You can see that the cache keys are different when we modify `mnist_fn` or `mnist_jax.py`
"""
from pathlib import Path

import nevergrad as ng

import daggerml as dml
from daggerml.executor import Sh

_here_ = Path(__file__).parent

params = ng.p.Dict(
    layer0_features=ng.p.Scalar(lower=16, upper=64).set_integer_casting(),
    layer1_features=ng.p.Scalar(lower=32, upper=128).set_integer_casting(),
    learning_rate=ng.p.Scalar(lower=1e-5, upper=0.5),
)

def mnist_fn(conf):
    from tempfile import TemporaryDirectory

    import mnist
    import numpy as np
    from ml_collections import ConfigDict
    config = ConfigDict(conf)
    with TemporaryDirectory(prefix='mnist-') as tmpd:
        result, loss, accuracy = mnist.train_and_evaluate(config, tmpd)
    # params_bytes = serialization.to_bytes(result.params)
    return {'loss': float(np.array(loss)), 'accuracy': float(np.array(accuracy))}

if __name__ == '__main__':
    base = {'num_epochs': 5, 'batch_size': 32, 'momentum': 0.9}
    params.random_state.seed(12)
    opt = ng.optimizers.registry['TwoPointsDE'](parametrization=params, budget=25)
    with dml.Dag.new('test-dag', 'testing nevergrad with jax in different envs') as dag:
        for i in range(opt.budget):
            x = opt.ask()
            p = x.value
            node = Sh(dag).run_hatch(mnist_fn,
                                     {**base, **p},
                                     mounts={'mnist.py': _here_/'assets/mnist_jax.py'},
                                     env='test-jax', cache=True)
            result = dag.get_value(node)
            print('='*80)
            print('iteration:', i + 1)
            print('params:', p)
            print('result:', result)
            print('='*80)
            opt.tell(x, -result['accuracy'])

        recommendation = opt.provide_recommendation()
        p = recommendation.value

        node = Sh(dag).run_hatch(mnist_fn,
                                 {**base, **p},
                                 mounts={'mnist.py': _here_/'assets/mnist_jax.py'},
                                 env='test-jax', cache=True)
        print('='*80)
        print('recommended parameters:', p)
        print('final result:', dag.get_value(node))
        print('='*80)
        dag.commit({'params': p, 'result': node})
