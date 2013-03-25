import os
import re

from setuptools import setup

v = open(os.path.join(os.path.dirname(__file__), 'calchipan', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.rst')

# Hack to prevent "TypeError: 'NoneType' object is not callable" error
# in multiprocessing/util.py _exit_function when running `python
# setup.py test` (see
# http://www.eby-sarna.com/pipermail/peak/2010-May/003357.html)
try:
    import multiprocessing
except ImportError:
    pass


setup(name='calchipan',
        version=VERSION,
        description="Crouching Alchemist Hidden Panda",
        long_description=open(readme).read(),
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy',
            'Topic :: Database :: Front-Ends',
        ],
        keywords='Pandas SQLAlchemy',
        author='Mike Bayer',
        author_email='mike@zzzcomputing.com',
        license='MIT',
        packages=['calchipan'],
        install_requires=['sqlalchemy'],
        include_package_data=True,
        tests_require=['nose >= 1.2.1'],
        test_suite="run_tests.setup_py_test", #sqlalchemy.testing.runner.setup_py_test",
        zip_safe=False,
        entry_points={
            'sqlalchemy.dialects': [
                'pandas = calchipan.base:PandasDialect',
                'pandas.calchipan = calchipan.base:PandasDialect',
            ]
        }
)
