import setuptools
from setuptools import setup
import subprocess
import os

remote_version = (
    subprocess.run(["git", "describe", "--tags"], stdout=subprocess.PIPE)
        .stdout.decode("utf-8")
        .strip()
)

if "-" in remote_version:
    # when not on tag, git describe outputs: "1.3.3-22-gdf81228"
    # pip has gotten strict with version numbers
    # so change it to: "1.3.3+22.git.gdf81228"
    # See: https://peps.python.org/pep-0440/#local-version-segments
    v, i, s = remote_version.split("-")
    remote_version = v + "+" + i + ".git." + s

assert "-" not in remote_version
assert "." in remote_version

assert os.path.isfile("nestedfunctions/version.py")
with open("nestedfunctions/VERSION", "w", encoding="utf-8") as fh:
    fh.write(f"{remote_version}\n")

# this grabs the requirements from requirements.txt
required_libs = {"pyspark"}
REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines() if i.split("==")[0] in required_libs]

setup(
    name='pyspark_nested_fields_functions',
    version=remote_version,
    author_email="golosegor@gmail.com",
    description="Utility functions to manipulate nested structures using pyspark",
    url="https://github.com/golosegor/pyspark-nested-fields-functions",
    python_requires=">=3.6",
    install_requires=[
        REQUIREMENTS
    ],
    package_data={"nestedfunctions": ["VERSION"]},
    packages=setuptools.find_packages(where='nestedfunctions'),
    include_package_data=True,
    package_dir={"": "nestedfunctions"}
)
