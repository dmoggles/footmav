"""Install packages as defined in this file into the Python environment."""
from setuptools import setup, find_namespace_packages

# The version of this tool is based on the following steps:
# https://packaging.python.org/guides/single-sourcing-package-version/
VERSION = {}

with open("./src/footmav/version.py") as fp:
    # pylint: disable=W0122
    exec(fp.read(), VERSION)

setup(
    name="footmav",
    author="Dmitry Mogilevsky",
    author_email="dmitry.mogilevsky@gmail.com",
    description="Football Maths and Visualisations Package",
    version=VERSION.get("__version__", "0.0.0"),
    package_dir={"": "src"},
    packages=find_namespace_packages(where="src", exclude=["tests"]),
    install_requires=[
        "setuptools>=45.0",
        "pandas",
        "requests",
        "beautifulsoup4==4.10.0",
        "attrs",
        "aiohttp",
        "asyncio",
        "tqdm",
        "scipy",
        "matplotlib==3.5.1",
        "highlight_text",
        "fuzzywuzzy",
        "python-Levenshtein"
    ],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Programming Language :: Python :: 3.0",
        "Topic :: Utilities",
    ],
)
