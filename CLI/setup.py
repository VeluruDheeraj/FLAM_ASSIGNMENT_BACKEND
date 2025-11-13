from setuptools import setup, find_packages

setup(
    name="queuectl",
    version="1.0.0",
    description="A CLI-based background job queue system with worker management and retry logic",
    author="Backend Developer",
    packages=find_packages(),
    install_requires=[
        "click>=8.1.7",
        "pydantic>=2.5.0",
    ],
    entry_points={
        "console_scripts": [
            "queuectl=queuectl.cli:main",
        ],
    },
    python_requires=">=3.8",
)
