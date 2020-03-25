import os
from setuptools import setup

# read in README
this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, 'README.md'), 'rb') as f:
    long_description = f.read().decode().strip()

# requirements
install_requires = [
    "adc >= 0.0.3",
    "dataclasses ; python_version < '3.7'",
    "xmltodict >= 0.9.0"
]
extras_require = {
    'dev': [
        'pytest >= 5.0, < 5.4',
        'pytest-console-scripts',
        'pytest-cov',
        'flake8',
        'flake8-black',
    ],
    'docs': [
        'sphinx',
        'sphinx_rtd_theme',
        'sphinxcontrib-programoutput',
    ],
}

setup(
    name = 'scimma-client',
    description = 'A client library for SCiMMA',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    url = 'https://github.com/scimma/client_library',
    author = 'Patrick Godwin',
    author_email = 'patrick.godwin@psu.edu',
    license = 'BSD 3-Clause',

    packages = ['scimma', 'scimma.client'],
    namespace_packages = ['scimma'],

    entry_points = {
        'console_scripts': [
            'scimma = scimma.client.__main__:main',
        ],
    },

    python_requires = '>=3.6.*',
    install_requires = install_requires,
    extras_require = extras_require,
    setup_requires = ['setuptools_scm'],
    use_scm_version = {
        'write_to': 'scimma/client/_version.py'
    },

    classifiers = [
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Astronomy',
        'Topic :: Scientific/Engineering :: Physics',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Operating System :: MacOS',
        'License :: OSI Approved :: BSD License',
    ],

)
