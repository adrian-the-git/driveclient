from setuptools import setup

setup(
    name='driveclient',
    version='v0.5.2',
    description='A simple Google Drive API Client which exposes basic features',
    url='http://github.com/adrian-the-git/driveclient',
    author='Adrian Carpenter',
    author_email='adriatic.c@gmail.com',
    license='Apache License, Version 2.0',
    packages=['driveclient'],
    install_requires=['google-api-python-client', 'PyCrypto']
)
