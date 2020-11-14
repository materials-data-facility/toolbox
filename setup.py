import os
from setuptools import setup

# Single source of truth for version
version_ns = {}
with open(os.path.join("mdf_toolbox", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns['__version__']

setup(
    name='mdf_toolbox',
    version=version,
    packages=['mdf_toolbox', 'mdf_toolbox.globus_search'],
    description='Materials Data Facility Python utilities',
    long_description=("Toolbox is the Materials Data Facility Python package"
                      " containing utility functions and other tools."),
    install_requires=[
        "fair-research-login>=0.1.5",
        "globus_nexus_client>=0.2.8",
        "globus-sdk>=1.7.0",
        "requests>=2.18.4"
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
