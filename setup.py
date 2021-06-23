from setuptools import setup, find_packages


with open("README.md") as f:
    long_description = f.read()


setup(
    name="ftgftm",
    version="0.1",
    description="Command-line client for Follow The Grant -> Follow The Money transformation",  # noqa
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Simon Wörpel",
    author_email="simon.woerpel@medienrevolte.de",
    url="https://gitlab.com/follow-the-grant/ftg-cli",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(),
    install_requires=[
        "Click<8.0.0",
        "alephclient",
        "countrytagger",
        "fingerprints",
        "followthemoney",
        "followthemoney-store",
        "html2text",
        "pandas",
        "pubmed_parser @ git+https://github.com/simonwoerpel/pubmed_parser.git@master",
        "spacy",
    ],
    entry_points={
        "console_scripts": ["ftgftm = ftgftm.cli:cli"],
    },
)
