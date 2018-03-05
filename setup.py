from setuptools import setup

setup(
    name='mdf_toolbox',
    version='0.1.6',
    packages=['mdf_toolbox'],
    description='Materials Data Facility Python utilities',
    long_description=("Toolbox is the Materials Data Facility Python package"
                      " containing utility functions and other tools."),
    install_requires=[
        "globus-sdk>=1.4.1",
        "requests>=2.18.4",
        "tqdm>=4.19.4",
        "six>=1.11.0"
    ],
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2",
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
