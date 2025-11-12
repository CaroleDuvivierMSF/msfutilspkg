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
        "numpy==2.3.3"
  ],
)
