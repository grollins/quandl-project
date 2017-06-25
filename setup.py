#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-quandl-stock-price',
      version='0.1.0',
      description='Singer.io tap for extracting stock prices from the Quandl API',
      author='Geoff Rollins',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_quandl'],
      install_requires=['singer-python>=0.1.0',
                        'backoff==1.3.2',
                        'requests==2.13.0'],
      entry_points='''
          [console_scripts]
          tap-quandl-stock-price=tap_quandl:main
      ''',
)
