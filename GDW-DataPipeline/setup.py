from setuptools import setup

setup(name='gdw-data-loader',
      version='1.0',
      description='************ GDW Data Loader application **************',
      author='venkat',
      py_modules=['__main__'],
      packages=['config','Tableloader','src','logs','utils'],zip_safe=False
      )
