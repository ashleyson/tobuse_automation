# tobuse_automation

## Overview
`tobuse_automation` is an automated pipeline for detecting tobacco use from clinical text data, specifically using VA Corporate Data Warehouse data. The project is designed to process clinical/medical notes/records and classify instances of tobacco usage using rule-based methods.

## Features
- **Preprocessing Pipeline**: Cleans and normalizes clinical text data.
- **Terminology-Based Detection**: Incorporates domain-specific dictionaries to improve accuracy.
- **Sequence Extraction**: Extracts relevant sequences from text to enhance classification.

## Repository Structure
```plaintext
├── classifier/          # Contains classification models and scripts
├── data/               # Dataset for training and evaluation
├── res/                # Resources such as dictionaries and headers
├── currsmok.py         # Script for detecting current smoking status
├── run_classifier.py   # Main script to run the classifier
├── output2.csv         # Sample output file
├── README.md           # Project documentation
```
**Installation**
**Prerequisites**
Ensure you have Python 3.7+ installed along with the required dependencies. 

**Setup**
1. Clone the repository:
```plaintext
   git clone https://github.com/ashleyson/tobuse_automation.git
   cd tobuse_automation
```
2. Install dependencies:
```plaintext
   pip install -r requirements.txt
```

**Usage**
To run the classifier and detect tobacco use from clinical text:
```
   python run_classifier.py --input data/input.csv --output results.csv --text_column ReportText
```
**Arguments**:
- ```--input```: Path to the input CSV file.
- ```--output```: Path to save the output CSV file.
- ```--text_columns```: Name of the column containing text data.

**Notes**:
- THe input file **must be in CSV format**. If using an Excel file, convert it to CSV or modify the script to use ```plaintext pd.read_excel()```.

**Contact**
For any questions, please contact yewon.son@gmail.com
   
