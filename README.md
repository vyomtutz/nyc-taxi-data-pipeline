# NYC Taxi Data Engineering Pipeline 🚖

This project demonstrates an end-to-end data engineering pipeline for NYC Taxi data, showcasing skills in ingestion, processing, orchestration, quality checks, and analytics using AWS and PySpark.

## 💡 Features

- Batch and streaming ingestion (Kafka, batch loaders)
- PySpark jobs for processing & cleaning
- Airflow orchestration with quality gates
- Data quality checks using Great Expectations & custom scripts
- Cloud storage integration (S3 buckets)

## 🗺️ Architecture

![Architecture](docs/architecture_diagram.png)

## 🚀 Tech Stack

- AWS S3, EMR, Athena
- PySpark
- Apache Kafka
- Apache Airflow
- Great Expectations

## ⚙️ How to run

1. Clone this repo
2. Install dependencies: `pip install -r requirements.txt`
3. Run Kafka producer to simulate streaming
4. Run PySpark jobs locally or on EMR
5. Trigger Airflow DAG

## 📄 License

MIT
