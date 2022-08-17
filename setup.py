import os
from setuptools import find_packages, setup

# read in README
this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, 'README.md'), 'rb') as f:
    long_description = f.read().decode().strip()

# requirements
install_requires = [
    "adc-streaming >= 2.1.0",
    "dataclasses ; python_version < '3.7'",
    "fastavro >= 1.4.0",
    "pluggy >= 0.11",
    "toml >= 0.9.4",
    "xmltodict >= 0.9.0"
]
extras_require = {
    'dev': [
        'autopep8',
        'flake8',
        'pytest >= 5.0, < 5.4',
        'pytest-console-scripts',
        'pytest-cov',
        'pytest-runner',
        'twine',
    ],
    'docs': [
        'sphinx',
        'sphinx_rtd_theme',
        'sphinxcontrib-programoutput',
    ],
}

setup(
    name = 'hop-client',
    description = 'A pub-sub client library for Multi-messenger Astrophysics',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    url = 'https://github.com/scimma/hop-client',
    author = 'Patrick Godwin',
    author_email = 'patrick.godwin@psu.edu',
    license = 'BSD 3-Clause',

    packages = find_packages(),

    entry_points = {
        'console_scripts': [
            'hop = hop.__main__:main',
        ],
    },

    python_requires = '>=3.6.*',
    install_requires = install_requires,
    extras_require = extras_require,
    setup_requires = ['setuptools_scm'],
    zip_safe=False,
    use_scm_version = {
        'write_to': 'hop/_version.py'
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
