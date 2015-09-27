from setuptools import setup

setup(
    name='driveclient',
    version='0.1',
    description='A simple Google Drive API Client which exposes minimal read-only features',
    url='http://github.com/adrian-the-git/driveclient',
    author='Adrian Carpenter',
    author_email='adriatic.c@gmail.com',
    license='Apache License, Version 2.0',
    packages=['driveclient'],
    install_requires=['google-api-python-client', 'PyCrypto']
)
