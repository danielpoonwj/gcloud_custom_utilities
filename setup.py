from setuptools import setup

setup(name='gcloud_custom_utilities',
      version='1.4',
      description='Wrapper around Google Cloud Apis',
      author='Daniel Poon',
      author_email='daniel.poon.wenjie@gmail.com',
      license='MIT',
      packages=['gcloud_custom_utilities'],
      install_requires=[
        'humanize',
        'pandas',
        'google-api-python-client>=1.5',
        'googleads',
        'unicodecsv',
        'pytz'
      ],
      zip_safe=False)
