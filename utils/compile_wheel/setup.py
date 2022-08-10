import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="provider_iwls",
    version="0.0.1",
    author="Maxime Carre",
    author_email="Maxime Carre",
    description="package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mcarre2/pygeoapi_iwls",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
