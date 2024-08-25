#! /usr/bin/python3

import re

from setuptools import find_packages, setup

with open('debian/changelog') as changelog:
    name, version = (
        re.compile('(\S+) \(([^\)~\s]+)[\)~]').match(changelog.readline()).group(1, 2)
    )

setup(
    name=name,
    version=version,
    packages=find_packages(),
    test_suite='tests',
)
