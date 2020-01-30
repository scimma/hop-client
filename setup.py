import os
from setuptools import setup

# read in README
this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, 'README.md'), 'rb') as f:
    long_description = f.read().decode().strip()

install_requires = []

setup(
    name = 'scimma-client',
    version = '0.0.1',
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

    python_requires='>=3.6.*',
    install_requires = install_requires,

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
