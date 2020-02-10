import os
import re
from setuptools import setup, find_packages

ROOT = os.path.dirname(__file__)
VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.]+)['"]''')

requires = [
    "boto3>=1.11.13"
]


def get_version():
    init = open(os.path.join(ROOT, 'boto3_large_message_utils', '__init__.py')).read()
    return VERSION_RE.search(init).group(1)


def readme():
    with open('README.md') as f:
        return f.read()


setup(name='boto3_large_message_utils',
      description='This library provides a way of bypassing AWS size restrictions when using services such as SQS and '
                  'SNS.',
      url='https://github.com/ajhaining/boto3_large_message_utils',
      license='MIT',
      author='Andrew Haining',
      author_email='haining.aj@gmail.com',
      packages=find_packages(exclude=['tests*']),
      version=get_version(),
      keywords='aws boto3 sqs sns kinesis',
      long_description=readme(),
      long_description_content_type='text/markdown',
      include_package_data=True,
      install_requires=requires)
