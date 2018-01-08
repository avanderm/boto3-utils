#!/usr/bin/env python

from distutils.util import convert_path
from setuptools import setup, find_packages

main_ns = {}
ver_path = convert_path('boto3_utils/version.py')
with open(ver_path) as ver_file:
    exec(ver_file.read(), main_ns)

setup(
    name='boto3_utils',
    version=main_ns['__version__'],
    description="Utility classes for interaction with AWS resources",
    url="https://github.com/avanderm/boto3-utils",
    author="Antoine Vandermeersch",
    author_email="avdmeers@gmail.com",
    license="BSD"
)