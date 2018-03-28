from setuptools import setup

setup(
    name='nio_ops',
    version='0.1',
    py_modules=['nio_ops'],
    install_requires=[
        'intervaltree',
        'six',
        'future',
    ],
    entry_points='''
        [console_scripts]
        numIoOps=nio_ops:io_operation_count
    ''',
)
