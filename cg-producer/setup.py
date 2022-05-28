#!/usr/bin/env python

from distutils.core import setup

def get_long_desc():
    with open("pypi-readme.md", "r") as readme:
        desc = readme.read()

    return desc

setup(name='pycg-producer',
      version='0.0.5',
      license='Apache Software License',
      long_description=get_long_desc(),
      long_description_content_type='text/markdown',
      description='Call Graph Producer for PyPI Packages with the use of PyCG ',
      author='Georgios-Petros Drosos',
      author_email='drosos007@gmail.com',
      url='https://github.com/fasten-project/pypi-tools/tree/main/cg-producer',
      packages=['pycg_producer'],
      include_package_data=True,
      install_requires=['pycg==0.0.5'],
     )