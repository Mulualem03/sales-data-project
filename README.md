
# 🛒 Sales Data ETL Pipeline

## 📑 Table of Contents
- [Project Overview](#project-overview)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [File Structure](#file-structure)
- [Technologies Used](#technologies-used)
- [License](#license)

---

## 📌 Project Overview

This project is an ETL (Extract, Transform, Load) pipeline built in Python to extract data from various sources (AWS RDS, PDFs, APIs, JSON, and S3), clean and normalize it, and load it into a PostgreSQL database hosted on AWS RDS.

The goal is to create a **star-based schema** that acts as a central source of truth for the company's sales operations.

### ✅ Objectives
- Extract raw data from multiple sources (RDS, S3, PDF, JSON, APIs)
- Clean and standardize the data using `pandas`
- Transform product weights, dates, and UUIDs
- Upload cleaned data to AWS RDS tables (`dim_` and `orders_table`)
- Automate and modularize the pipeline for reusability

---

## ⚙️ Installation Instructions

```bash
# Clone the repository
git clone git@github.com:Mulualem03/sales-data-project.git
cd sales-data-project

# Recreate the virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
## 🚀 Usage Instructions

1. **Set up AWS CLI credentials**:
   ```bash
   aws configure
   ```

2. **Run the pipeline step by step or all at once**:
   ```bash
   python main.py
   ```

   Or execute scripts/modules as needed for:
   - Extracting from `RDS` (`legacy_users`, `legacy_store_details`)
   - PDF extraction via `tabula`
   - API pull using `requests`
   - JSON/S3 file reads using `boto3` or `pandas`

3. **Check your AWS RDS database** to confirm that the following tables were uploaded:
   - `dim_users`
   - `dim_store_details`
   - `dim_card_details`
   - `dim_products`
   - `orders_table`
   - `dim_date_times`

---

## 📁 File Structure

```
sales-data-project/
├── data_cleaning.py
├── data_extraction.py
├── database_utils.py
├── main.py
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🛠️ Technologies Used

- Python 3.8
- pandas
- SQLAlchemy
- psycopg2
- tabula-py
- boto3
- AWS RDS & S3
- Git + GitHub

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

