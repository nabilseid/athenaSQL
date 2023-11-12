# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import sys
import os

import sphinx

sys.path.insert(0, os.path.abspath('../athenaSQL'))

project = 'athenaSQL'
copyright = '2023, Nabil Seid'
author = 'Nabil Seid'
version = sphinx.__display_version__
release = version

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'myst_nb',
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    "sphinx.ext.napoleon",
    'sphinx.ext.autosummary'
]

source_suffix = {
    '.rst': 'restructuredtext',
    '.ipynb': 'myst-nb',
    '.myst': 'myst-nb',
}

nb_execution_mode = 'off'

numpydoc_show_class_members = False

# Look at the first line of the docstring for function and method signatures.
autodoc_docstring_signature = True
autosummary_generate = True

autodoc_default_options = {
    'inherited-members': False
}

# autoapi_add_toctree_entry = False
# autoapi_dirs = ["../athenaSQL"]  # location to parse for API reference

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# --- Code block styling ---

pygments_style = "sphinx"
pygments_dark_style = "monokai"

# --- Options for HTML output ---
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_title = "athenaSQL"
html_favicon = '_static/favicon.ico'

html_static_path = ['_static']

html_css_files = [
    'css/athenasql.css',
    'css/autosummary.css'
]
