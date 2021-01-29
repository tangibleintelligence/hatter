from setuptools import setup, find_packages

with open("README.md", encoding = "utf-8") as readme:
    LONG_DESCRIPTION = readme.read()

setup(
    name = "hatter",
    version = "0.0.1",
    description = "Framework to easily create microservices backed by a RabbitMQ broker",
    long_description = LONG_DESCRIPTION,
    author = "Austin Howard",
    author_email = "austin@tangibleintelligence.com",
    packages = find_packages('src'),
    package_dir = {'': 'src'}
)
