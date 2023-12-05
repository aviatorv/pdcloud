from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_desc = f.read()

setup(
    name="pdcloud",
    version="0.4.1",
    packages=find_packages(),
    description="Python pandas dataframe cloud agnostic storage",
    long_description=long_desc,
    long_description_content_type="text/markdown",
    author="Vitali M.",
    author_email="pilot@aviatorv.com",
    url="",
    license="MIT",
    install_requires=[
        "azure-storage-blob",
        "pyarrow",
        "pandas",
        "aiohttp",
    ],
    extras_require={"test": ["pytest", "pytest-asyncio", "twine"]},
    python_requires=">=3.8",
)
