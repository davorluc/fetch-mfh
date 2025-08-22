# fetch-mfh

This project fetches and processes **Baugesuche (building applications)** from the Swiss cantons of **Zürich (ZH)** and **Zug (ZG)**, filtering specifically for **Mehrfamilienhäuser (multi-family houses)**.  
It automatically collects applicant names and addresses from the official [Amtsblattportal API](https://amtsblattportal.ch/api/v1/publications), stores them in a CSV file, and uploads the data to Google Sheets for easy access.

## Features

- Fetches Baugesuche from cantons ZH and ZG using the public Amtsblattportal API  
- Parses XML metadata and publication content to extract:
  - Applicant name  
  - Applicant address  
  - Relevant details confirming whether it is a Mehrfamilienhaus  
- Saves all results into a CSV file (`baugesuche_ZH_ZG_MFH.csv`)  
- Uploads and appends data to a Google Sheet on Google Drive  
- GitHub Actions workflow:
  - Runs daily at 18:00  
  - Installs dependencies  
  - Executes fetch script and saves CSV  
  - Uploads data to Google Sheets  
  - Stores new entries as downloadable artifacts

## Repository Structure

```

fetch-mfh/
├── fetch\_baugesuche\_mehrfamilienhaus\_ZH\_ZG.py   # Fetch and parse Baugesuche, save to CSV
├── sheets\_uploader.py                           # Upload CSV data to Google Sheets
├── baugesuche\_ZH\_ZG\_MFH.csv                     # Example output dataset
├── requirements.txt                             # Python dependencies for GitHub Actions
├── shell.nix                                    # Development environment (Python + tools)
└── .github/workflows/                           # CI/CD pipeline configuration

````

## Getting Started

### Prerequisites
- Dependencies listed in `requirements.txt`  
- (Optional) [Nix](https://nixos.org/) for reproducible development environment  

### Local Setup
Clone the repository and install dependencies:
```bash
git clone https://github.com/davorluc/fetch-mfh.git
cd fetch-mfh
pip install -r requirements.txt
````

Or, using Nix:

```bash
nix-shell
```

### Usage

Run the fetcher to create a CSV file:

```bash
python fetch_baugesuche_mehrfamilienhaus_ZH_ZG.py
```

Upload results to Google Sheets:

```bash
python sheets_uploader.py
```

### Automated Workflow

The included GitHub Actions workflow runs daily at **18:00** and:

* Prepares the Python environment
* Runs the fetcher script
* Uploads data to Google Sheets
* Publishes new entries as GitHub artifacts


## Notes

This repository is a showcase of using:

* REST APIs (Amtsblattportal)
* XML parsing and data extraction
* Google Cloud APIs (Drive/Sheets)
* GitHub Actions automation
* Reproducible development environments (Nix)
```
