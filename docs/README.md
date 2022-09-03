# Configuring Sphinx to build API documentation

See `conf.py` for Sphinx extensions and settings.

See `index.rst` and `api.rst` for the placement of the `.. autosummary::` directive and its `:recursive:` option.

# Configuring Sphinx to integrate Jupyter Notebooks

For more information, see `notebooks/README`.

# Building this HTML doc set locally

You can clone this repo and build and view the API and tutorial documentation locally:

1. Change to the `docs` directory:

   `cd docs`

2. Build the documentation:

   `make html`

3. Run a web server:

   `python -m http.server`

4. View the doc set locally in a browser at:

   http://localhost:8000/_build/html/
