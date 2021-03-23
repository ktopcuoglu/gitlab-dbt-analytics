from setuptools import setup

requires = [
    "dbt==0.18.1",
    "gitlabdata==0.2.1",
    "pandas==0.25.3",
    "requests==2.22.0",
    "urllib3==1.24.0",
]

setup(
    name="GitLab dbt dev",
    version="1.0",
    description="dbt module for development",
    author="Taylor Murphy, Michael Walker",
    author_email="mwalker@gitlab.com",
    install_requires=requires,
)
