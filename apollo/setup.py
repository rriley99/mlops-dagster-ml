from setuptools import find_packages, setup

setup(
    name="apollo",
    packages=find_packages(exclude=["apollo_tests"]),
    install_requires=[
        "dagster==1.6.*",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-aws",
        "pandas",
        "plotly",
        "shapely",
        "sklearn"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
