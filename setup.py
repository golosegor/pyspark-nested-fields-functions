from setuptools import setup, find_packages

# this grabs the requirements from requirements.txt
required_libs = {"pyspark"}
REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines() if i.split("==")[0] in required_libs]


setup(
    name='recursive_utils',
    version='2022.5.1',
    include_package_data=True,
    package_data={
        "": ["*.json"],
    },
    install_requires=[
        REQUIREMENTS
    ],
    packages=find_packages(
        where='src'
    ),
    package_dir={"": "src"}
)
