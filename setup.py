from setuptools import setup
from setuptools import find_packages

setup(
    name='latencyconverter',
    version='0.9.0',
    description='Utility to convert latency information from csv(grlp) and \
                json(nnm) formats to compressed hdf5 format for long-term \
                storage.',
    author='Jonathan Gosset',
    author_email='jonathan.gosset@nrcan-rncan.gc.ca',
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
    },
    entry_points={
        'console_scripts': [
            'lat_to_hdf5 = latencyconverter.bin.lat_to_hdf5: main',
            'daily_lat_store = latencyconverter.bin.daily_lat_storage: main'
        ]
    }
)
