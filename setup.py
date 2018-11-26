from setuptools import setup

setup(
    name='gtfs',
    version='2.0.1',
    py_modules=['gtfs'],
    install_requires=[
        'pandas',
        'shapely',
        'numpy'
    ]
)