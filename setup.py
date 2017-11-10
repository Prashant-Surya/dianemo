from setuptools import setup
from pip.req import parse_requirements

install_reqs = parse_requirements('requirements.txt', session='hack')
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='dianemo',
    version='0.0.1',
    packages=['dianemo'],
    author='Dianemo',
    install_requires=reqs
)
