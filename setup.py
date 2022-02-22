from setuptools import setup
from setuptools import find_packages

setup(
    name='latencyfileconverter',
    version='0.1.0',
    description='Utility to convert latency information from csv(grlp) and \
                json(nnm) formats to compressed hdf5 format for long-term \
                storage.',
    author='Jonathan Gosset',
    author_email='jwj.gosset@gmail.com',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'h5py'
    ],
    extras_require={
        'dev': [
            'pytest',
            'mypy',
            'flake8'
        ]
    }
)
