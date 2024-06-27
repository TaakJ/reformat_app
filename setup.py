from setuptools import setup, find_packages

setup(
    name="uob-app", 
    version="0.1",
    packages=find_packages(),
    description="build package uob-app",
    entry_point={"console_script":["reformat_app:uob-app:main"]},
    )