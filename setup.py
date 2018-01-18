# -*- coding: utf-8 -*-

import os
from setuptools import setup

rootpath = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    return open(os.path.join(rootpath, *parts), 'r').read()


def extract_version(module='factornado'):
    version = None
    fname = os.path.join(rootpath, module, '__init__.py')
    with open(fname) as f:
        for line in f:
            if (line.startswith('__version__')):
                _, version = line.split('=')
                version = version.strip()[1:-1]  # Remove quotation characters.
                break
    return version


def walk_subpkg(name):
    data_files = []
    package_dir = 'factornado'
    for parent, dirs, files in os.walk(os.path.join(package_dir, name)):
        # Remove package_dir from the path.
        sub_dir = os.sep.join(parent.split(os.sep)[1:])
        for f in files:
            data_files.append(os.path.join(sub_dir, f))
    return data_files


pkg_data = {'': []}
pkgs = ['factornado', ]

LICENSE = read('LICENSE.txt')
long_description = '{}\n{}'.format(read('README.rst'), read('CHANGES.txt'))

# Dependencies.
with open('requirements.txt') as f:
    tests_require = f.readlines()
install_requires = [t.strip() for t in tests_require]


config = dict(name='factornado',
              version=extract_version(),
              description='Factory for creating microservices with tornado',
              long_description=long_description,
              author='Martin Journois',
              author_email='martin@journois.fr',
              url='https://github.com/factornado/factornado',
              keywords='microservices web tornado',
              classifiers=['Programming Language :: Python :: 2.7',
                           'Programming Language :: Python :: 3.4',
                           'Programming Language :: Python :: 3.5',
                           'License :: OSI Approved :: MIT License',
                           'Development Status :: 5 - Production/Stable'],
              packages=pkgs,
              package_data=pkg_data,
              setup_requires=['pytest-runner', ],
              tests_require=['pytest'],
              license=LICENSE,
              install_requires=install_requires,
              zip_safe=False)


setup(**config)
