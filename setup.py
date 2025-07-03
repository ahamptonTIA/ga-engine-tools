from setuptools import setup, find_packages

setup(
	name='ga-engine-tools',
	version='0.1.0',
	packages=find_packages(),
	install_requires=[
		# 'pandas>=1.0.0',
		# 'requests>=2.20.0', 
		'arcgis>=2.4.1',
		# 'pyspark>=3.0.0', # Note: PySpark is often provided by the environment (e.g., Databricks)
		# 'packaging>=21.0', # Required for version parsing in compatibility check
		# 'geoanalytics
		'html2text',
		],
        author='ahamptonTIA', 
        author_email='ahamptonTIA@users.noreply.github.com', # Left empty intentionally, all project communication should be directed via GitHub Issues as per README.md.
	description='A Python library offering a collection of utilities for common data '
				'operations, with a primary focus on managing ArcGIS GIS connections '
				'and robustly publishing large-scale data to ArcGIS Feature Services '
				'using PySpark.',
	long_description=open('README.md').read(),
	long_description_content_type='text/markdown',
	url='https://github.com/ahamptonTIA/ga-engine-tools', # GitHub repository URL
	classifiers=[
		'Programming Language :: Python :: 3',
		'Programming Language :: 3.8',
		'Programming Language :: 3.9',
		'Programming Language :: 3.10',
		'Programming Language :: 3.11',
		'Programming Language :: 3.12',
		'License :: OSI Approved :: MIT License',
		'Operating System :: OS Independent',
		'Development Status :: 3 - Alpha',
		'Intended Audience :: Developers',
		'Topic :: Scientific/Engineering :: GIS',
		'Topic :: Software Development :: Libraries :: Python Modules',
	],
	keywords='esri geoanalytics engine gis spatial analysis big data',
	python_requires='>=3.8',
)
