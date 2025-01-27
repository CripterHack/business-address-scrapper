# Web Scraper Project

This project is a comprehensive web scraping tool designed to extract specific business information from the web. It leverages the power of Scrapy for efficient crawling, integrates OCR for captcha solving, and utilizes a local LLaMA model to enhance search queries. The scraper reads a list of business names from a CSV file, searches for relevant information, and outputs the data into a structured CSV format.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Troubleshooting and Debugging](#troubleshooting-and-debugging)
- [Testing](#testing)
- [Contributing](#contributing)
- [To-Do Checklist](#to-do-checklist)

## Features

- **Efficient Web Crawling**: Utilizes Scrapy for high-performance web scraping.
- **Captcha Solving**: Integrates Tesseract-OCR for solving captchas encountered during scraping.
- **Enhanced Search Queries**: Uses a local LLaMA model to refine search queries for better accuracy.
- **Proxy Rotation**: Implements proxy rotation to avoid IP bans and mimic human-like browsing.
- **CSV Input/Output**: Reads business names from a CSV file and outputs the scraped data into a CSV file.

## Prerequisites

Before running the project, ensure you have the following installed:

- **Python 3.x**: The programming language used for the project.
- **Tesseract-OCR**: For captcha solving.
  - **Windows**: [Download and install Tesseract-OCR](https://github.com/UB-Mannheim/tesseract/wiki).
  - **Linux/macOS**: Install via package manager:
    ```bash
    sudo apt install tesseract-ocr
    ```
- **Google Chrome**: Required for Selenium to simulate browser interactions.
- **ChromeDriver**: Compatible with your Chrome version. [Download here](https://sites.google.com/a/chromium.org/chromedriver/).

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/web-scraper-project.git
   cd web-scraper-project
   ```

2. **Set Up a Virtual Environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows, use venv\Scripts\activate
   ```

3. **Install Required Python Packages**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Tesseract Path**:
   - **Windows**: Ensure the Tesseract executable is in your system's PATH or specify the path in `scraper/captcha_solver.py`:
     ```python
     pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
     ```

## Usage

1. **Prepare the Input CSV**:
   - Create a file named `businesses.csv` with a single column `business_name` containing the names of businesses you want to scrape.

2. **Run the Scraper**:
   ```bash
   python run_scraper.py
   ```

3. **View the Output**:
   - The scraped data will be saved in `business_data.csv` in the project directory.

## Project Structure

```
web-scraper-project/
│
├── scraper/
│   ├── spiders/
│   │   └── business_spider.py
│   ├── __init__.py
│   ├── captcha_solver.py
│   ├── llama_processor.py
│   ├── pipelines.py
│   └── settings.py
│
├── businesses.csv
├── run_scraper.py
├── requirements.txt
└── README.md
```

- **scraper/spiders/business_spider.py**: Contains the Scrapy spider for crawling and scraping data.
- **scraper/captcha_solver.py**: Handles captcha solving using Tesseract-OCR.
- **scraper/llama_processor.py**: Integrates the local LLaMA model for query refinement.
- **scraper/settings.py**: Configures Scrapy settings, including proxy rotation and download delays.
- **run_scraper.py**: Main script to execute the scraper.
- **businesses.csv**: Input file containing business names to scrape.
- **requirements.txt**: Lists all Python dependencies.

## Troubleshooting and Debugging

- **Common Issues**:
  - **Captcha Solving Failures**: Ensure Tesseract-OCR is correctly installed and configured. Test it separately to confirm it's working.
  - **Proxy Errors**: Verify that the proxies listed in `scraper/settings.py` are active and correctly formatted.
  - **Missing Dependencies**: Double-check that all required Python packages are installed by reviewing `requirements.txt`.

- **Debugging Tips**:
  - **Logging**: Implement logging within your scripts to capture detailed information about the scraping process.
  - **Interactive Debugging**: Use Python's built-in `pdb` module to set breakpoints and inspect variables during execution.

## Testing

- **Unit Tests**: Develop unit tests for individual functions, especially for captcha solving and data extraction methods.
- **Integration Tests**: Test the entire scraping workflow with a subset of business names to ensure all components work together seamlessly.
- **Mocking**: Use libraries like `unittest.mock` to simulate network responses and test how your scraper handles different scenarios.

## Contributing

We welcome contributions to enhance this project. To contribute:

1. **Fork the Repository**: Click the "Fork" button at the top right of this page.
2. **Create a New Branch**: Use a descriptive name for your branch.
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make Your Changes**: Implement your feature or fix.
4. **Commit Your Changes**: Write clear and concise commit messages.
   ```bash
   git commit -m "Description of your changes"
   ```
5. **Push to Your Fork**:
   ```bash
   git push origin feature/your-feature-name
   ```
6. **Submit a Pull Request**: Navigate to the original repository and submit a pull request from your fork.

## To-Do Checklist

- [ ] **Enhance Error Handling**: Improve exception handling to make the scraper more robust.
- [ ] **Implement Advanced Captcha Solving**: Integrate services like 2Captcha for more complex captchas.
- [ ] **Add More Proxies**: Expand the list of proxies to reduce the risk of IP bans.
- [ ] **Optimize Data Extraction**: Refine the parsing logic to handle a wider variety of website structures.
- [ ] **Expand Testing Suite**: Develop more comprehensive tests to cover edge cases and ensure reliability.
- [ ] **Improve Documentation**: Add detailed explanations of each module and function within the codebase.

By following this guide, you should be well-equipped to set up, run, and contribute to the web scraper project. If you encounter any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request. 