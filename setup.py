#!/usr/bin/env python

from setuptools import setup, find_packages
import oscar_views

setup(
    name='oscar_views',
    version=oscar_views.__version__ ,
    description='library implementing some dedicated OSCAR/Surface views',
    author='Timo Proescholdt',
    author_email='tproescholdt@wmo.int',
    url='https://github.com/kurt-hectic/oscar-views',
    packages=find_packages(),
    install_requires=[
        'setuptools',
        'requests',
        'pandas',
        'pycountry'
      ],
    
    )