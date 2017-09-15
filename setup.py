from setuptools import setup

setup(
    name="numerai",
    author="Numerai",
    install_requires=[
        "boto3",
        "botocore",
        "pandas==0.20.3",
        "tqdm",
        "pymongo",
        "scipy",
        "sklearn",
        "statsmodels",
        "python-dotenv",
        "bottle",
        "numpy",
        "pqueue",
        "randomstate",
    ],
)
