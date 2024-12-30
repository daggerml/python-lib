.. _howto:

How-To Guide
============

This guide will walk you through the most useful aspects of DaggerML, including creating and managing DAGs, using the API, and running tests.

Creating a DAG
--------------

To create a new DAG, you can use the `Dag` class from the `daggerml.core` module. Here is an example:

.. code-block:: python

    import daggerml as dml

    # Create a new DAG
    dag = dml.new(name="example_dag", message="Example DAG creation")

    # Add nodes to the DAG
    node1 = dag.put(3)
    node2 = dag.put("example")
    node3 = dag.put([node1, node2])

    # Commit the DAG
    dag.commit(node3)

Using the API Class
-------------------

The `Api` class provides methods to interact with the DAGs. Here is an example of how to use the `Api` class:

.. code-block:: python

    import daggerml as dml

    # Create an API instance
    api = dml.Api(config_dir="path/to/config/directory")

    # Create a new DAG using the API
    dag = api.new_dag(name="example_dag", message="Example DAG creation")

    # Add nodes to the DAG
    node1 = dag.put(3)
    node2 = dag.put("example")
    node3 = dag.put([node1, node2])

    # Commit the DAG
    dag.commit(node3)

Loading and Dumping a DAG
-------------------------

You can load and dump a DAG using the `load` and `dump` methods of the `Dag` class:

.. code-block:: python

    from daggerml.core import Dag

    # Load a DAG from a file
    dag = Dag.load("path/to/dag_file")

    # Dump the DAG to a file
    dag.dump("path/to/dag_file")

Conclusion
----------

This guide provided an overview of the most useful aspects of DaggerML, including creating and managing DAGs, using the API, and running tests. For more detailed information, refer to the API documentation and the test suite.