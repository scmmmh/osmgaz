from setuptools import setup, find_packages
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()


version = '0.1'

requires = ['shapely',
            'sqlalchemy',
            'psycopg2',
            'geoalchemy2',
            'pyproj']


setup(name='OSMGaz',
      version=version,
      description="OSM Gazetteer",
      long_description=README + '\n\n' + NEWS,
      classifiers=[
                   ],
      keywords='',
      author='Mark Hall',
      author_email='Mark.Hall@work.room3b.eu',
      license='',
      packages=find_packages('src'),
      package_dir = {'': 'src'},
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      entry_points = """\
      [console_scripts]
      OSMGaz = osmgaz:main
      """,
)
