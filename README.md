# 🏃‍♂️ Global Athletics Competitions Data Enrichment Pipeline

This repository provides a fully automated pipeline for collecting, processing, and enriching global athletics competition data. It integrates historical competition data from [World Athletics](https://worldathletics.org/), adds geolocation (latitude & longitude) using the OpenCage Geocoding API, and finally enriches it with environmental data (e.g., PM2.5, temperature, rainfall) from NASA EarthData.

---

## 🧹 Project Overview

The project consists of three main scripts that must be run in sequence:

### 1. `update-athlete.py`

- Scrapes global athletics competition data from the [World Athletics](https://worldathletics.org/) website.
- Retrieves detailed competition data via their GraphQL API.
- Parses and stores each competition's metadata and result data to disk (as CSV and JSON).
- Removes duplicates and aggregates all data into a single CSV file.

### 2. `main.py`

- Extracts geolocation (latitude & longitude) of competition venues using the [OpenCageData API](https://opencagedata.com/).
- Generates a list of all competition dates (including days before the competition).
- Prepares data for integration with environmental datasets.

### 3. `NASA_full_automatic.py`

- Downloads selected NASA Earth observation data (e.g., PM2.5, temperature, ozone) based on the competitions' date and location.
- Converts NASA `.nc4` NetCDF files to CSV format.
- Merges NASA data with competition records to enrich the dataset with relevant environmental conditions at the time of each competition.

---

## 🚀 How to Run the Pipeline

### 📍 Step 1: Scrape and Prepare Competition Data

```bash
python update-athlete.py
```

### 📍 Step 2: Add Geolocation and Generate Date Range

```bash
python main.py get-dates
# OR
python main.py cities     # To fetch lat/lng for venues based on city name
python main.py address    # To fetch lat/lng for more precise venue names
```

### 📍 Step 3: Download and Merge NASA Environmental Data

```bash
python NASA_full_automatic.py
```

> Note: You must update `NASA_full_automatic.py` with the correct file name (`file_name`) for your desired dataset, and ensure your NASA EarthData cookies are valid.

---

## 📦 Folder Structure

- `json_<date>/` – JSON files of competition metadata from World Athletics
- `data_<date>/` – Individual CSV files for each competition
- `result/` – Final merged and cleaned data files
- `NASA_downloads/` – Raw NASA NetCDF files
- `country/` – CSVs of country-specific competition locations with NASA grid coordinates

---

## 🌍 Supported NASA Parameters

The following environmental parameters can be extracted and merged:

| Parameter  | NASA Product  | Description                  |
| ---------- | ------------- | ---------------------------- |
| `PM2.5`    | MERRA-2 (aer) | Particulate matter           |
| `T2MMEAN`  | MERRA-2 (slv) | Mean 2-meter air temperature |
| `TPRECMAX` | MERRA-2 (slv) | Max precipitation            |
| `O3`       | MERRA-2 (ana) | Ozone and wind profiles      |
| `Rainfall` | FLDAS/GLDAS   | Rainfall data                |

---

## 🔧 Dependencies

- Python ≥ 3.8
- `pandas`, `numpy`, `aiohttp`, `aiofiles`, `requests`, `xarray`, `tqdm`, `bs4`

You can install dependencies using:

```bash
pip install -r requirements.txt
```

> Note: Ensure `xarray` and `netCDF4` are correctly set up for reading `.nc4` files.

---

## 🔑 API & Credentials

- **OpenCage API Key:** Replace the `API_KEY` in `main.py` with your own.
- **NASA EarthData Cookies:** You must provide valid cookies to access protected datasets. Set them in the `EARTHDATA_COOKIES` environment variable; never paste them into the code.

---

## Data

- **Sources:** competition calendars and results from the World Athletics website and its GraphQL endpoint (`update-athlete.py`); venue coordinates from the OpenCage Geocoding API (`main.py`); environmental variables from NASA EarthData download lists stored under `NASA/` (`NASA_full_automatic.py`).
- **Local inputs the scripts expect:** `payload.json` (GraphQL query settings), `updated_data.csv` (results from the previous scrape), `raw_data.dta` or `data_with_city_name.csv` (for `main.py`), NASA URL lists in `NASA/`, and country files in `country/` with `lat`, `lon` and city name columns.
- **Not in the repository:** none of these inputs or any outputs are included. `.gitignore` excludes `.csv`, `.dta`, `.xlsx`, `.parquet` and `.nc` files. Competition results include athlete-level records, so treat them as personal data.
- `requirements.txt` is now included for the `pip install -r requirements.txt` command above.

---

## Running time

Not recorded.

---

## 📊 Use Case Example

> Want to know the PM2.5 level on the date and location of each competition?

Just run the pipeline and include `'PM2.5'` as the target parameter from NASA. The final merged CSV will contain an added `PM2.5` column.

---

## 📬 Contact

Created by **Mahyar Rezazadeh Khormiz**.  
If you have questions or would like to contribute, feel free to open an issue or pull request.

---
