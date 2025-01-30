from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="business-address-scraper",
    version="1.0.0",
    author="Edgar Zorrilla",
    description="A web scraper for extracting business information from New York",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/business-address-scraper",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        'dev': [
            'pytest>=7.0',
            'pytest-cov>=4.0',
            'black>=22.0',
            'isort>=5.0',
            'mypy>=1.0',
            'flake8>=6.0',
            'pre-commit>=3.0',
        ],
        'test': [
            'pytest>=7.0',
            'pytest-cov>=4.0',
            'pytest-mock>=3.0',
            'responses>=0.23',
        ],
    },
    entry_points={
        'console_scripts': [
            'run-scraper=scraper.run_scraper:main',
        ],
    },
    include_package_data=True,
    zip_safe=False,
) 