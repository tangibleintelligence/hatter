from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as readme:
    LONG_DESCRIPTION = readme.read()

setup(
    name="hatter",
    version="0.4.2",
    description="Framework to easily create microservices backed by a RabbitMQ broker",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="Austin Howard",
    author_email="austin@tangibleintelligence.com",
    packages=find_packages("src"),
    package_dir={"": "src"},
    python_requires=">3.8",
    install_requires=[
        "aio-pika>=7.1.0,<8",
        "pydantic>=1.7.3,<2",
    ],
)
