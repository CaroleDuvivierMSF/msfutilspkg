from setuptools import setup, find_packages

setup(
    name="msfutilspkg",       # package name
    version="0.1.0",              # version
    packages=find_packages(),     # automatically find packages
    description="Helper functions for Fabric notebooks",
    python_requires=">=3.8",
    install_requires=[
        "deltalake==1.2.1",
        "sqlalchemy==2.0.43",
        "pandas==2.3.3",
        "numpy==2.3.3",
        "matplotlib==3.10.8",
        "xlutils==2.0.0",
        "psycopg2-binary==2.9.10",
        "pyspark==4.0.1",
        "pyarrow==21.0.0"
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=7.0.0"  # optional for coverage reports
        ]
    }
)
