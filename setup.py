from setuptools import setup

setup(
    name='mdf_toolbox',
    version='0.2.4',
    packages=['mdf_toolbox'],
    description='Materials Data Facility Python utilities',
    long_description=("Toolbox is the Materials Data Facility Python package"
                      " containing utility functions and other tools."),
    install_requires=[
        "globus_nexus_client>=0.2.8",
        "globus-sdk>=1.5.0",
        "requests>=2.18.4",
        "tqdm>=4.19.4"
    ],
    python_requires=">=3.4",
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
