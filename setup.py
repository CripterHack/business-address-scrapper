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
    packages=find_packages(exclude=["tests*", "scripts*"]),
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
        'ai': [
            'torch>=2.1.2',
            'transformers>=4.37.2',
            'llama-cpp-python>=0.2.23',
            'PyQt5>=5.15.11',
            'PyQt5-sip>=12.17.0',
            'PyQt5-Qt5>=5.15.16',
            'PyQtWebKit>=5.15.6'
        ]
    },
    entry_points={
        'console_scripts': [
            'run-scraper=scraper.run_scraper:main',
            'setup-scraper=scripts.setup:main',
            'check-prod=scripts.check_prod_config:main',
            'deploy-scraper=scripts.deploy:main'
        ],
    },
    package_data={
        'scraper': [
            'config/*.yaml',
            'config/*.json',
            'data/templates/*.html',
            'data/static/css/*.css',
            'data/static/js/*.js'
        ]
    },
    include_package_data=True,
    zip_safe=False,
) 