import os
from setuptools import setup, find_packages

# Initialize pathing for README integration
here = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = "A Python library for ArcGIS and PySpark data operations."

setup(
    name='ga-engine-tools',
    version='1.0.0',
    author='ahamptonTIA',
    author_email='ahamptonTIA@users.noreply.github.com',
    description='Utilities for ArcGIS GIS connections and PySpark data operations.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/ahamptonTIA/ga-engine-tools',
    
    # Automatic discovery of ga_engine_tools package structure
    packages=find_packages(),
    
    # Aligned with Databricks Runtime (DBR) 15.x which utilizes Python 3.11
    python_requires='>=3.10',

    # CORE FUNCTIONAL DEPENDENCIES
    # Minimum versions defined to ensure API compatibility.
    # Open-ended ranges allow for existing environment versions to be utilized without force-upgrading.
    install_requires=[
        'arcgis>=2.1.4',
        'geopandas>=1.0.0',
        'html2text>=2020.1.16',
        'geoanalytics>=1.0.0',
    ],

    # OPTIONAL/ENVIRONMENTAL DEPENDENCIES
    # Core libraries provided by Databricks Runtime (Pandas, PySpark, etc.) are moved here
    # to prevent installation-time conflicts and kernel restart warnings.
    extras_require={
        'local': [
            'pandas>=2.0.0',
            'numpy>=1.23.0',
            'pyspark>=3.5.0',
            'packaging>=24.0',
        ],
    },

    # Metadata and Categorization
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: GIS',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Operating System :: OS Independent',
    ],
    
    keywords='esri arcgis geoanalytics databricks pyspark gis spatial',
    
    project_urls={
        'Documentation': 'https://github.com/ahamptonTIA/ga-engine-tools#readme',
        'Source': 'https://github.com/ahamptonTIA/ga-engine-tools',
        'Tracker': 'https://github.com/ahamptonTIA/ga-engine-tools/issues',
    },
)