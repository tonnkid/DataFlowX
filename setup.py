from setuptools import setup, find_packages

setup(
    name="dataflowx",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "httpx>=0.27.0",
        "aiohttp>=3.9.0",
        "pydantic>=2.5.0",
        "redis>=5.0.0",
        "kafka-python>=2.0.0",
        "prometheus-client>=0.19.0",
        "structlog>=23.0.0",
    ],
    python_requires=">=3.11",
)
