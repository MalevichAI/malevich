# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys

sys.path.insert(0, '../')

import malevich

release = open(os.path.join('../', 'VERSION')).read().strip()

project = 'Malevich'
copyright = '2024, Malevich AI, Co'
author = 'Aleksandr Lobanov'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# extensions = []
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.coverage',
    'sphinx.ext.napoleon',
    'sphinxcontrib.mermaid'
]

intersphinx_mapping = {
    'pandas': ('https://pandas.pydata.org/docs/reference/api', None),
}
templates_path = ['_templates']
exclude_patterns = []

html_title = f'Malevich {release}'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'pydata_sphinx_theme'
html_static_path = ['_static']
html_favicon = 'favicon.ico'
html_css_files = [
    'style.css',
]

mermaid_verbose = True
autodoc_member_order = 'bysource'
autosectionlabel_prefix_document = True
