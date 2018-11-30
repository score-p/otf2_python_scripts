from setuptools import setup

setup(
    name='otf2_access_stats',
    version='0.1',
    py_modules=['otf2_access_stats'],
    install_requires=[
        'six',
        'future',
        'intervaltree',
        'mypy'
    ],
)
