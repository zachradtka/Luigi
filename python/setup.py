try:
    from setuptools import setup
except:
    from distutils.core import setup

# Workaround for nose bug from: http://bugs.python.org/issue15881
try:
    import multiprocessing
except ImportError:
    pass

from distutils.core import setup

setup(name='amazon_web_service-luigi',
      version='0.1.0',
      description='AWS Extensions for Luigi',
      author='Mortar Data',
      author_email='info@mortardata.com',
      namespace_packages = [
          'amazon_web_service'
      ],
      packages=[
          'amazon_web_service.luigi'
      ],
      license='LICENSE.txt',
      install_requires=[
          'luigi',
          'boto',
      ]
)
