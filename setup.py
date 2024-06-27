from setuptools import setup, find_packages

setup(
    name="reformat_app", 
    version="0.1",
    packages=find_packages(),
    description="build package reformat uob app",
    entry_points={"console_scripts":["reformat_app:reformat_app:main"]},
    )