# Market Data Analytics Pipeline

## Project Overview

This project is an entry-level data engineering portfolio project that builds a financial market ETL pipeline using 
Python, PostgreSQL, and Docker.

The pipeline extracts daily stock price data from the Alpha Vantage API, stores the raw API response in a Bronze layer, 
transforms the data into a clean Silver layer, and creates a Gold analytics layer for business reporting.

The project simulates a real business data workflow where raw external data is collected, stored, cleaned, validated, 
and prepared for analysis.

## Project Purpose

The purpose of this project is to demonstrate core data engineering skills in a realistic business scenario.

This project shows how to:

- Build a containerized ETL pipeline.
- Extract data from an external API.
- Load raw data into PostgreSQL.
- Transform raw data into structured analytical tables.
- Use a Bronze/Silver/Gold warehouse design.
- Add logging and retry handling.
- Add data quality validation.
- Query analytics-ready data with SQL.

## Business Problem

Financial analysts need reliable market data to monitor stock performance, compare daily price changes, and identify 
trends over time.

Raw API data is difficult to use directly because it is nested, unstructured, and not organized for reporting. A data 
pipeline is needed to convert the raw API response into structured tables that can be queried with SQL.

## Business Solution

This project solves that problem by creating a repeatable ETL workflow that:

- Extracts daily stock data from the Alpha Vantage API.
- Stores the original API response for traceability and auditability.
- Transforms raw market data into clean daily price records.
- Creates business-ready analytics metrics such as daily return, volatility, price change, and trend direction.
- Makes the final data available in PostgreSQL for SQL analysis and reporting.

## How To Run

### 1. Clone Or Download The Project

Download or copy the project files to your computer.

### 2. Create An Alpha Vantage API Key

This project uses the Alpha Vantage API as the stock market data source.

Create a free API key from Alpha Vantage:

```text
https://www.alphavantage.co/support/#api-key
```

Each user should use their own API key.

### 3. Create A Local `.env` File

This project uses environment variables for secrets, so API keys and passwords are not written directly inside the 
Python files or Docker Compose file.

```env
API_KEY=your_real_alpha_vantage_api_key
POSTGRES_PASSWORD=your_own_postgres_password
```

### 4. Start The Docker Pipeline

Open PowerShell/terminal in the project folder:

Run:

```powershell
docker compose up --build
```

Docker Compose will read the `.env` file, start PostgreSQL, build the Python ETL container, and run the pipeline.

### 5. Connect With pgAdmin

After the containers are running, connect to the Docker PostgreSQL database in pgAdmin:

```text
Host: localhost
Port: 5433
Database: testDB
Username: postgres
Password: the POSTGRES_PASSWORD value from your .env file
```

## Project Structure

```text
DESystemDesign/
|-- docker-compose.yml
|-- Dockerfile
|-- requirements.txt
|-- main.py
|-- extract.py
|-- warehouse.py
|-- queries.sql
`-- README.md
```


## File Responsibilities

```text
main.py
Runs the full ETL pipeline and controls the workflow.

extract.py
Handles API extraction, retry logic, and rate limiting.

warehouse.py
Creates PostgreSQL schemas and tables, loads Bronze/Silver/Gold data, and runs data quality checks.

docker-compose.yml
Starts the PostgreSQL database container and the Python ETL container.

Dockerfile
Builds the Python ETL container.

requirements.txt
Lists the Python packages needed by the project.

queries.sql
Contains useful SQL queries for validating and exploring the Bronze, Silver, and Gold layers.
```

## Architecture

```text
Alpha Vantage API
        |
        v
Python ETL Container
        |
        v
PostgreSQL Database
        |
        v
Bronze Schema -> Silver Schema -> Gold Schema
        |
        v
SQL Analysis / pgAdmin
```

## Data Flow

1. The Python ETL pipeline requests daily stock price data from the Alpha Vantage API.
2. The raw API response is loaded into the Bronze schema.
3. The raw response is transformed into clean daily stock price records in the Silver schema.
4. Data quality checks validate that records were loaded correctly.
5. The Gold schema calculates analytics-ready metrics for reporting.
6. The final data can be queried in pgAdmin or any PostgreSQL SQL client.

## Medallion Architecture

This project uses a Bronze/Silver/Gold Medallion Architecture.

### Bronze Layer

The Bronze layer stores the raw API response exactly as received from Alpha Vantage.

This layer supports:

- Raw data retention
- Auditability
- Traceability
- Reprocessing if transformation logic changes later

### Silver Layer

The Silver layer stores cleaned and structured daily stock price records.

This layer includes:

- Symbol
- Trade date
- Open price
- High price
- Low price
- Close price
- Volume

### Gold Layer

The Gold layer stores analytics-ready financial metrics.

This layer includes:

- Daily price change
- Daily return percentage
- Daily volatility
- Trend direction
- Previous close price

## Useful SQL Queries

Useful validation and analysis queries are stored in `queries.sql`.

These queries can be run in pgAdmin after connecting to the Docker PostgreSQL database.
