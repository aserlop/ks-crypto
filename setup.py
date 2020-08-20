from setuptools import setup, find_packages
import os


with open(os.path.join(os.path.dirname(__file__),
                       'requirements.txt')) as requirements:
    install_requires = requirements.readlines()

setup(
    name='ks_crypto',
    version='0.0.9999',
    packages=find_packages(exclude=['tests', 'tests.*', 'notebooks', 'notebooks.*']),
    include_package_data=True,
    install_requires=install_requires,
    package_data={'ks_crypto': []},
)