from setuptools import setup, find_packages

setup(
    name="reformat_app",
    version="0.1",
    packages=find_packages(),
    description="build package reformat uob app",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[r.strip() for r in open("requirements.txt").readlines()],
    python_requires=">=3.9",
)
