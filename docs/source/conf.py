import datetime

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from typing import Any

sys.path.insert(0, os.path.abspath("../.."))
from retentioneering import __version__

# -- Project information -----------------------------------------------------
actual_year = datetime.datetime.now().year

project = "Retentioneering"
copyright = f"2018-{actual_year}, Maxim Godzi"
author = "Maxim Godzi"

# The full version, including alpha/beta/rc tags
release = __version__

# The short X.Y version
dev = ["b", "rc", "a"]
if any([x in release for x in dev]):
    version = ".".join(release.split(".")[:2]) + " (dev)"
else:
    version = __version__

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "numpydoc",
    "sphinx.ext.viewcode",  # add link to source code
    "sphinx.ext.extlinks",
    "sphinx.ext.autosectionlabel",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_togglebutton",
    "sphinx.ext.todo",
]

# -- Options for HTML output -------------------------------------------------
autodoc_member_order = "groupwise"
numpydoc_show_inherited_class_members = False
numpydoc_class_members_toctree = False
numpydoc_show_class_members = False
html_theme = "pydata_sphinx_theme"
html_favicon = "_static/favicon.ico"
todo_include_todos = False
todo_emit_warnings = True

html_context = {
    "default_mode": "light",
    "github_user": "retentioneering",
    "github_repo": "retentioneering-tools",
    "github_version": "docs_fixes",
    "doc_path": "docs/source",
}

html_theme_options = {
    "logo": {"image_light": "rete_logo.svg", "image_dark": "rete_logo_white.svg"},
    "show_toc_level": 2,
    "github_url": "https://github.com/retentioneering/retentioneering-tools",
    "icon_links": [
        {
            "name": "Discord",
            "url": "https://discord.gg/hBnuQABEV2",
            "icon": "fab fa-discord",
            "type": "fontawesome",
        },
        {
            "name": "Telegram",
            "url": "https://t.me/retentioneering_support",
            "icon": "fab fa-telegram",
            "type": "fontawesome",
        },
    ],
    "use_edit_page_button": True,
    "header_links_before_dropdown": 6,
    # "check_switcher": False,
    "navbar_end": ["version-switcher", "theme-switcher", "navbar-icon-links"],
    "switcher": {
        "json_url": "https://doc.retentioneering.com/versions.json",
        "version_match": version,
    },
    "footer_items": ["copyright", "sphinx-version", "footer_extension"],
    "analytics": {
        "google_analytics_id": "G-M0RGDZCYGM",
    },
}

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

source_suffix = ".rst"

# The master toctree document.
master_doc = "index"

# Make sure the automatically created targets are unique
autosectionlabel_prefix_document = True

html_css_files = [
    "css/custom.css",
]

html_js_files = [("https://static.server.retentioneering.com/static/retedocs.min.js", {"async": "async"})]

# This is a solution for deeply nested lists while building the doc as a pdf-file.
# https://stackoverflow.com/a/28454426
latex_elements = {"preamble": "\\usepackage{enumitem}\\setlistdepth{99}"}

rst_epilog = """
.. raw:: html

    <style>
        .alarm {color:#24ff83; font-weight:bold;}
    </style>

.. role:: alarm

.. |warning| replace:: ⚠️

"""

extlinks = {
    "numpy_link": ("https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-units/%s", None),
    "sklearn_tfidf": (
        "https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html%s",
        None,
    ),
    "sklearn_countvec": (
        "https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html%s",
        None,
    ),
    "numpy_timedelta_link": (
        "https://numpy.org/doc/stable/reference/arrays.datetime.html"
        "#:~:text=There%%20are%%20two,numbers%%20of%%20days/%s",
        None,
    ),
    "plotly_autosize": ("https://plotly.com/python/reference/layout/#layout-autosize/%s", None),
    "plotly_width": ("https://plotly.com/python/reference/layout/#layout-width/%s", None),
    "plotly_height": ("https://plotly.com/python/reference/layout/#layout-height/%s", None),
    "mannwhitneyu": ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.mannwhitneyu.html%s", None),
    "scipy_chi2": ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.chi2_contingency.html%s", None),
    "scipy_fisher": ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.fisher_exact.html%s", None),
    "scipy_ks": ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.ks_2samp.html%s", None),
    "statsmodel_ttest": (
        "https://www.statsmodels.org/dev/generated/statsmodels.stats.weightstats.ttest_ind.html%s",
        None,
    ),
    "statsmodel_ztest": ("https://www.statsmodels.org/dev/generated/statsmodels.stats.weightstats.ztest.html%s", None),
    "numpy_random_choice": (
        "https://numpy.org/doc/stable/reference/random/generated/numpy.random.RandomState.choice.html%s",
        None,
    ),
    "numpy_random_seed": ("https://numpy.org/doc/stable/reference/random/generated/numpy.random.seed.html%s", None),
    "pandas_copy": (
        "https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html"
        "#:~:text=If%%20None%%2C%%20infer.-,copybool,-or%%20None%%2C%%20default%s",
        None,
    ),
    "sklearn_kmeans": ("https://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html%s", None),
    "sklearn_gmm": ("https://scikit-learn.org/stable/modules/generated/sklearn.mixture.GaussianMixture.html%s", None),
    "sklearn_tsne": ("https://scikit-learn.org/stable/modules/generated/sklearn.manifold.TSNE.html%s", None),
    "umap": ("https://umap-learn.readthedocs.io/en/latest/api.html%s", None),
    "numpy_bins_link": (
        "https://numpy.org/doc/stable/reference/generated/numpy.histogram_bin_edges.html#numpy.histogram_bin_edges%s",
        None,
    ),
    "matplotlib_axes": ("https://matplotlib.org/stable/api/axes_api.html#matplotlib.axes.Axes%s", None),
}


def setup(app: Any) -> None:
    app.add_css_file("css/custom.css")
    app.add_css_file("css/dataframe.css")
