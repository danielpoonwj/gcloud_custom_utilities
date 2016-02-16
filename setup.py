from setuptools import setup

setup(name='gcloud_custom_utilities',
      version='1.2',
      description='Wrapper around Google Cloud Apis',
      author='Daniel Poon',
      author_email='daniel.poon.wenjie@gmail.com',
      packages=['gcloud_custom_utilities'],
      install_requires=[
        'humanize',
        'pandas',
        'google-api-python-client',
        'googleads'
      ])
