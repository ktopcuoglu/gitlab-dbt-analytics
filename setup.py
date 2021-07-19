from setuptools import setup

requires = [
    "dbt==0.19.1",
    "gitlabdata==0.2.1",
    "pandas==0.25.3",
    "requests==2.22.0",
    "urllib3==1.24.0",
]

setup(
    name="GitLab dbt dev",
    version="1.0",
    description="dbt module for development",
    author="GitLab Data",
    author_email="analyticsapi@gitlab.com",
    install_requires=requires,
)
