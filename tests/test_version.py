#!/usr/bin/env python

__author__ = "Shereen Elsayed (s_elsayed@ucsb.edu)"
__description__ = "Test for version command"

import subprocess
from hop import version

def test_version():
    process = subprocess.run(['hop', 'version'], 
                            stdout=subprocess.PIPE,
                            universal_newlines=True)
    
    versions =  process.stdout
    packages = version.get_packages()
    assert any(package in versions for package in packages)
