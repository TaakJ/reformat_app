from setuptools import setup, find_packages

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()
    
install_requires = [r.strip() for r in open("requirements.txt").readlines()]

setup(
    name="reformat_app",
    version="0.0.1",
    packages=find_packages(),
    description="build package reformat uob app",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=install_requires,
    long_description=long_description,
    long_description_content_type='text/markdown',
    # entry_points={
    #     "console_scripts":[
    #         "reformat-app = reformat_app:main",
    #     ]
    # },
    python_requires=">=3.9",
)
