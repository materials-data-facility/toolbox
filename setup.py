import os
from setuptools import setup, find_packages

# Single source of truth for version
version_ns = {}
with open(os.path.join("mdf_toolbox", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns['__version__']

setup(
    name='mdf_toolbox',
    version=version,
    packages=find_packages(),
    description='Materials Data Facility Python utilities',
    long_description=("Toolbox is the Materials Data Facility Python package"
                      " containing utility functions and other tools."),
    install_requires=[
        "fair-research-login>=0.2.4",
        "globus_nexus_client>=0.4.1",
        "globus-sdk>=3.1.0",
        "requests>=2.26.0",
        "jsonschema>=4.3.0"
    ],
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
    ],
    keywords=[
        "MDF",
        "Materials Data Facility",
        "materials science",
        "utility"
    ],
    license="Apache License, Version 2.0",
    url="https://github.com/materials-data-facility/toolbox"
)
