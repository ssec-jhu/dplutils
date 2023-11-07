# Configuration file for the Sphinx documentation builder.

# -- Project information
from dplutils import __project__, __version__

project = __project__
copyright = '2023, SSEC-JHU'
author = 'SSEC-JHU'

release = __version__
version = __version__

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx.ext.graphviz',
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

suppress_warnings = ['epub.unknown_project_files']

# -- Options for HTML output

html_theme = 'sphinx_book_theme'
html_static_path = ['../_static']
html_css_files = ['../_static/custom.css']
html_logo = '../_static/SSEC_logo_vert_white_lg_1184x661.png'
html_title = f'{project} {release}'
html_theme_options = {
    'logo': {
        'image_light': '../_static/SSEC_logo_horiz_blue_1152x263.png',
        'image_dark': '../_static/SSEC_logo_vert_white_lg_1184x661.png',
        'text': f'{html_title}',
    },
    'repository_url': 'https://github.com/ssec-jhu/dplutils',
    'use_repository_button': True,
    'navigation_with_keys': False,
 }

# -- Options for EPUB output
epub_show_urls = 'footnote'
