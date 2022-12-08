# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

import datetime

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from typing import Any

sys.path.insert(0, os.path.abspath("../.."))


# -- Project information -----------------------------------------------------
actual_year = datetime.datetime.now().year

project = "Retentioneering"
copyright = f'2018-{actual_year}, "Data Driven Lab" LLC'
author = '"Data Driven Lab" LLC'

# The short X.Y version
version = ""
# The full version, including alpha/beta/rc tags
release = "3.0.0"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "numpydoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.extlinks",
]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
numpydoc_show_inherited_class_members = False
numpydoc_class_members_toctree = False
numpydoc_show_class_members = False
html_theme = "pydata_sphinx_theme"

html_logo = "rete_logo.png"
autodoc_typehints = "none"
# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
pygments_style = "sphinx"
# -- Options for HTMLHelp output ---------------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = "RetentioneeringToolsdoc"

# Add any paths that contain _templates here, relative to this directory.
templates_path = ["_templates"]
# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
source_suffix = ".rst"

# The master toctree document.
master_doc = "index"

extlinks = {
    "numpy_link": ("https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-units", None),
    "sklearn_tfidf": (
        "https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html",
        None,
    ),
    "sklearn_countvec": (
        "https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html",
        None,
    ),
    "numpy_timedelta_link": (
        "https://numpy.org/doc/stable/reference/arrays.datetime.html#:~:text=There%20are%20two,numbers%20of%20days.",
        None,
    ),
    "plotly_autosize": ("https://plotly.com/python/reference/layout/#layout-autosize", None),
    "plotly_width": ("https://plotly.com/python/reference/layout/#layout-width", None),
    "plotly_height": ("https://plotly.com/python/reference/layout/#layout-height", None),
    "mannwhitneyu": ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.mannwhitneyu.html", None),
    "scipy_chi2": ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.chi2_contingency.html", None),
    "scipy_fisher": ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.fisher_exact.html", None),
    "scipy_ks": ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.ks_2samp.html", None),
    "statsmodel_ttest": (
        "https://www.statsmodels.org/dev/generated/statsmodels.stats.weightstats.ttest_ind.html",
        None,
    ),
    "statsmodel_ztest": ("https://www.statsmodels.org/dev/generated/statsmodels.stats.weightstats.ztest.html", None),
}


def setup(app: Any) -> None:
    app.add_stylesheet("css/custom.css")
