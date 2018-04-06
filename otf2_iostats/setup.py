from setuptools import setup

setup(
    name='otf2_iostats',
    version='0.1',
    py_modules=['otf2_iostats'],
    install_requires=[
        'python >= 3.4',
        'intervaltree',
        'six',
        'future',
    ],
)
