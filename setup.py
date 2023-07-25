from setuptools import find_packages, setup

setup(
    name="bitcointalk4de40ec26",
    version="0.0.3",
    packages=find_packages(),
    install_requires=[
        "exorde_data",
        "aiohttp",
        "beautifulsoup4>=4.11"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
