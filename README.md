# Consumer Complaints Automated ETL Pipeline

An end-to-end data engineering pipeline built with **Apache Airflow** to automate the extraction, transformation, and loading of consumer financial complaint data.


## 🚀 Project Overview
This project automates the workflow of fetching real-time consumer complaints from the Consumer Finance API, processing the data for insights, and delivering it to stakeholders via Google Sheets and Email alerts.

## 🛠 Tech Stack
- **Orchestration:** Apache Airflow
- **Language:** Python (Pandas)
- **Database:** MySQL
- **External APIs:** Consumer Finance API, Google Sheets API
- **Monitoring:** EmailOperator (Airflow)

## 🔄 Workflow
1. **Extract:** Fetches 30 days of data for all US states via REST API.
2. **Load (Raw):** Stores raw JSON/CSV data into a MySQL database for persistence.
3. **Transform:** Cleans data, handles nulls, and aggregates complaints by product, issue, and state using Pandas.
4. **Sensor:** Airflow FileSensor waits for the successful creation of the transformed file.
5. **Load (Cloud):** Syncs final data to a Google Spreadsheet for live reporting.
6. **Notify:** Sends an automated email with the Spreadsheet link upon success.


## 🛠 Execution Flow & Expected Outputs

When you run this pipeline, the following steps will happen automatically:

### 1. Run `extract.py`
* **Output:** The script fetches consumer complaint data from the official API for the last 30 days and saves it as a temporary raw file.

### 2. Run `load.py` (Part 1)
* **Output:** A table named `Consumer_Complaints` is automatically created in your **MySQL database**, and the raw data is securely stored in it.

### 3. Run `transform.py`
* **Output:** The raw data is pulled from MySQL, cleaned (handling nulls and duplicates), and summarized into a new file called `consumer_complaints_transformed.csv`.

### 4. Run `load.py` (Part 2)
* **Output:** The cleaned and summarized data is automatically uploaded to your connected **Google Sheet** for live reporting.

### 5. Airflow Success Email
* **Output:** Upon successful completion, the system sends an **automated email** containing the direct link to the final Google Sheet report.

