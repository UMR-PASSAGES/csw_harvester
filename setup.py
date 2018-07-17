# coding=utf-8
from distutils.core import setup


setup(name='csw-harvester',
      version='2.0',
      description='Software that extracts metadata from the Catalogue Service Web for analysis',
      author='Université de Bordeaux et CNRS',
      author_email='bruno.pinaud@u-bordeaux.fr',
      url='http://geobs.cnrs.fr/',
      packages=['database', 'sources', 'src', 'test'],
      scripts=['src/Main.py', 'test/Coverage', 'test/Profiling'],
      data_files=[('Log', ['csw-harvester.log']),
                  ('test_results', ['test/coverageTest, test/profile_results.prof_file']),
                  ('config', ['config_database.cfg']),
                  ('README', ['README.md'])],
      )
