"""Compatibility metadata for the older pip bundled with the Spark image."""

from setuptools import find_packages, setup

setup(
    name="openfoodfacts-cluster",
    version="0.2.0",
    description="PySpark clustering pipeline for Open Food Facts",
    python_requires=">=3.10",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["numpy>=1.26,<3", "requests>=2.32,<3"],
    extras_require={
        "local": ["pyspark==3.5.9"],
        "dev": ["pytest>=8.3,<9", "pytest-cov>=6,<8", "ruff>=0.11,<1"],
    },
)
