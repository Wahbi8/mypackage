from setuptools import setup, find_packages

setup(
    name='data_ingestion',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy',
        'pandas'
    ],
    author='Oussama Wahbi',
    author_email='wahbi.oussama08@gmail.com',
    description='A package for data ingestion tasks',
    long_description='A package containing functions for ingesting data from databases and web sources.',
    url='https://github.com/Wahbi8/data_ingestion',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
