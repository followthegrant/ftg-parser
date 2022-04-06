from setuptools import setup, find_packages


with open("README.md") as f:
    long_description = f.read()


setup(
    name="followthegrant-parser",
    version="0.1",
    description="Parser and command-line client for Follow The Grant -> Follow The Money transformation",  # noqa
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Simon Wörpel",
    author_email="simon.woerpel@medienrevolte.de",
    url="https://gitlab.com/follow-the-grant/followthegrant-parser",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(),
    package_dir={"followthegrant": "ftg"},
    install_requires=[
        "Click",
        "alephclient",
        "countrytagger",
        "dataset",
        "dateparser",
        "fingerprints",
        "followthemoney",
        "followthemoney-store",
        "html2text",
        "pandas",
        "pubmed_parser @ git+https://github.com/simonwoerpel/pubmed_parser.git@master",  # noqa
        "pyicu",
        "pydantic",
        "spacy",
        "pyyaml<6.0.0",
        "pyparsing<3",
        "fasttext",
        "networkx",
        "ingest @ git+https://github.com/alephdata/ingest-file.git",
        "servicelayer @ git+https://github.com/simonwoerpel/servicelayer.git@mark_errors",
    ],
    entry_points={
        "console_scripts": ["ftg = ftg.cli:cli"],
    },
)
