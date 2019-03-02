#! /usr/bin/python3

from setuptools import setup, find_packages
import re

with open('debian/changelog') as changelog:
	name, version = re.compile('(\S+) \(([^\)~\s]+)[\)~]').match(changelog.readline()).group(1, 2)

setup(
	name = name,
	version = version,
	packages = find_packages(),
	test_suite = 'tests',
)
