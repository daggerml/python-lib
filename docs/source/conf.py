# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import daggerml as dml

project = "daggerml"
copyright = "2025, Aaron Niskin and Micha Niskin"
author = "Aaron Niskin and Micha Niskin"
release = version = dml.__version__

toc_object_entries = True

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",  # Add links to highlighted source code
    "sphinx_autodoc_typehints",  # For type annotations
    "myst_parser",  # For markdown support
]

templates_path = ["_templates"]
exclude_patterns = []

# Optional: Automatically document class members
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}
autodoc_typehints = "description"
autodoc_class_signature = "separated"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

autosummary_generate = True

html_static_path = ["_static"]
html_logo = "_static/logo-nobg.png"
html_title = "DaggerML Documentation foopy"
html_show_sourcelink = False
html_sidebars = {
    "**": [
        "navbar-logo.html",
        "icon-links.html",
        "search-button-field.html",
        "sbt-sidebar-nav.html"
    ]
}

# html_theme = "pydata_sphinx_theme"
html_theme = "sphinx_book_theme"
html_theme_options = {
    "repository_url": "https://github.com/daggerml/python-lib",
    "use_repository_button": True,
    "home_page_in_toc": True,
    # "show_toc_level": 1,
    # # "show_nav_level": 4,
    # "collapse_navigation": False,
}