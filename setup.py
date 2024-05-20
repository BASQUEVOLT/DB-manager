# -*- coding: utf-8 -*-
"""
Created on Mon May 20 08:28:41 2024

@author: EugenioCalandrini
"""

from setuptools import find_packages, setup

setup(
    name="DBconnector",
    version="0.0.0",
    packages=find_packages(),
    description="Utility to connect to the neware server and fetch the data",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Eugenio Calandrini",
    author_email="ecalandrini@basquevolt.com",
    url="https://github.com/BASQUEVOLT/DB-manager",
    install_requires=[
        "mysql-connector",
        "pandas",
        "numpy",
    ],
    classifiers=[
        # Full list at https://pypi.org/classifiers/
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)