from setuptools import setup

setup(
    name='driveclient',
    version='2.0.0',
    description='A simple Google Drive, Docs, and Sheets API client',
    url='http://github.com/adrian-the-git/driveclient',
    author='Adrian Carpenter',
    author_email='adriatic.c@gmail.com',
    license='Apache License, Version 2.0',
    packages=['driveclient'],
    python_requires='>=3.8',
    install_requires=[
        'google-api-python-client>=2.0',
        'google-auth>=2.0',
        'google-auth-oauthlib>=1.0',
        'google-auth-httplib2>=0.1',
    ],
    extras_require={
        'pandas': ['pandas>=1.0'],
    },
)
