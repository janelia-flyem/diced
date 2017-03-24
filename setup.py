from setuptools import setup

packages=['DVIDCloudStore'
         ]

package_data={}

setup(name='DVIDCloudStore',
      version='0.1',
      description='Enable access to large, cloud-based nD data volumes',
      url='https://github.com/janelia-flyem/DVIDCloudStore',
      packages=packages,
      package_data=package_data
      )
