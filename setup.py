from setuptools import setup, find_packages

setup(
    name='uob_reformat',
    version='0.13',
    description='build package reformat uob app',
    long_description=open('README.md').read() + '\n\n' + open('CHANGELOG.txt').read(),
    long_description_content_type='text/markdown',
    url='',
    author='MFEC',
    author_email='',
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    keywords='',
    packages=find_packages(),
    python_requires=">=3.9",
    # install_requires=[r.strip() for r in open("requirements.txt").readlines()],
    entry_points={
        "console_scripts":[
            "uob_reformat = uob_reformat:main",
        ]
    },
)
