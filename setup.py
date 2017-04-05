from setuptools import setup

packages=['diced'
         ]

setup(name='diced',
      version='0.2',
      description='Enable access to large, cloud-based nD data volumes',
      url='https://github.com/janelia-flyem/DVIDCloudStore',
      packages=packages,
      include_package_data=True,
      test_suite="diced.tests"
      )
