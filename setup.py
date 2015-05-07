#!/usr/bin/env python

from distutils.core import setup

setup(name='Celery-sopnet',
      version='0.1',
      description='Celery tasks for Sopnet',
      author='Tom Kazimiers',
      author_email='tom@voodoo-arts.net',
      url='https://github.com/catsop/celery-sopnet',
      packages=['celerysopnet'],
      install_requires=['celery', 'pysopnet'],
      dependency_links=[
        "git+https://github.com/catsop/python-sopnet.git#egg=pysopnet-0.1",
      ],
)
