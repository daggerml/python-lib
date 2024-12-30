.. _contributing:

Contributing Guide
==================

Welcome to the DaggerML contributing guide! This document will help you get
started with contributing to the DaggerML project. It covers everything from
setting up your development environment to running tests and finally
contributing code.

Setting Up Your Development Environment
---------------------------------------

1. **Clone the repository with submodules**

   .. code-block:: sh

      git clone --recurse-submodules https://github.com/daggerml/python-lib.git
      cd python-lib

2. **Install hatch**
    See `hatch documentation <https://hatch.pypa.io/latest/>`_ for how you want
    to install hatch. We suggest using the `pipx installation
    <https://hatch.pypa.io/latest/install/#pipx>`_ method.

   .. code-block:: sh

      python3 -m pip install --user pipx
      python3 -m pipx ensurepath
      pipx install hatch

3. **Install daggerml-cli**

   .. code-block:: sh

      pipx install daggerml-cli

4. **Install dependencies**

   Some of the tests run parts of the computation in other environments, and
   those requirements have dependencies.

   * **Docker**

     Some tests require `Docker <https://docs.docker.com/get-docker/>`_. Make
     sure Docker is installed and running on your machine.

   * **Conda**
   
     Some tests require `Conda
     <https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html>`_.
     Make sure Conda is installed on your machine. Once conda is installed,
     create a conda environment called `torch` that has `pytorch
     <https://pytorch.org/get-started/locally/>`_ installed, then install
     `daggerml` in editable mode in that environment.

     .. code-block:: sh

        conda create -n torch python=3.10
        conda activate torch
        conda install pytorch -c pytorch
        pip install -e .

5. **Run tests**

   The tests run in a test (hatch) environment defined in `pyproject.toml`. You
   can run the tests using the following command:

   .. code-block:: sh

      hatch run test:test

   Some of the tests run parts of the execution in Docker containers, other
   hatch environments, or Conda environments. If you see errors related to
   these, you may need to install Docker or Conda.

Building Documentation
----------------------

To build the documentation, use the following command:

.. code-block:: sh

   hatch run docs:build-docs

Contributing Code
-----------------

1. **Find an issue to work on**

   Browse the `GitHub Issues <https://github.com/daggerml/python-lib/issues>`_ to find an issue you'd like to tackle.

2. **Create a branch**

   The branch name should be descriptive of the changes you are making and start
   with the issue number. For example, if you are working on issue #123 and
   adding a new feature, you could name your branch ``123-new-feature``.

   .. code-block:: sh

      git checkout -b your-branch-name

3. **Make your changes**

4. **Commit and push your changes**

   .. code-block:: sh

      git add .
      git commit -m "Your commit message"
      git push origin your-branch-name

5. **Create a pull request**

   Go to the `GitHub repository <https://github.com/daggerml/python-lib>`_ and
   create a pull request. Set @amniskin or @micha as the reviewer.

Conclusion
----------

Thank you for contributing to DaggerML! Your contributions help make this
project better for everyone. If you have any questions, feel free to reach out
on GitHub.

Happy coding!