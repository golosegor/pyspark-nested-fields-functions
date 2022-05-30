from setuptools import setup, find_packages

# this grabs the requirements from requirements.txt
required_libs = {"pyspark"}
REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines() if i.split("==")[0] in required_libs]


setup(
    name='recursive_utils',
    version='2022.5.1',
    include_package_data=True,
    author_email="golosegor@gmail.com",
    description="Utility functions to manipulate nested structures using pyspark",
    url="https://github.com/golosegor/pyspark-nested-fields-functions",
    package_data={
        "": ["*.json"],
    },
    python_requires=">=3.6",
    install_requires=[
        REQUIREMENTS
    ],
    packages=find_packages(
        where='nestedfunctions'
    ),
    package_dir={"": "nestedfunctions"}
)
