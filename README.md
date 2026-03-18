# Modular Insurance ETL Pipeline

A custom-built, production-ready ETL (Extract, Transform, Load) pipeline designed to process messy CSV data, validate it against strict business rules, and safely load it into a MySQL database. 

I built this project to bridge the gap between basic scripting and industry-standard software engineering. Instead of just writing a script that "works," I focused on making a system that fails gracefully, logs its actions, and protects the database from bad data.

## 🛠️ Tech Stack
* **Language:** Python 3.x
* **Database:** MySQL
* **Libraries:** `mysql-connector-python` (Database driver), `python-dotenv` (Environment variable management)
* **Architecture:** Object-Oriented Programming (OOP), Singleton Pattern, Context Managers

## 💡 Key Engineering Decisions

Here is a breakdown of why I structured the code the way I did:

* **Zero `print()` Statements:** The system uses Python's `logging` module exclusively. Every event, warning, and critical failure is time-stamped and written to an `etl_process.log` file. 
* **Object-Oriented Validation:** Data rules (like checking for negative premium amounts or empty policy IDs) are separated into individual classes using inheritance. If a row fails, the pipeline logs a warning and skips the row without crashing the entire system.
* **Safe Database Connections:** I used a Python Context Manager (`__enter__` and `__exit__`) for the MySQL connection. If the program crashes halfway through reading a file, the context manager guarantees the database connection is closed and any partial, corrupted inserts are rolled back.
* **Staging to Master Architecture:** The SQL script (`report.sql`) doesn't just dump data. It loads data into a raw `staging` table first, uses `INSERT IGNORE` to safely move unique records to a `master` table, and then truncates staging. This makes the pipeline idempotent (you can run it 100 times without duplicating data).
* **Advanced SQL:** Included a reporting view utilizing Window Functions (`DENSE_RANK()`) to analyze regional claim data.

## 📂 Project Structure

```text
insurance_etl_system/
├── config/
│   └── db_config.py       # Fetches credentials from environment variables
├── data/
│   ├── incoming/          # Drop raw .csv files here
│   └── processed/         # Successfully processed files are moved here automatically
├── logs/
│   └── etl_process.log    # Auto-generated diary of system events
├── src/
│   ├── __init__.py
│   ├── database.py        # Safe MySQL connection manager
│   ├── ingestion.py       # Orchestrates reading, validating, and loading
│   └── validation.py      # OOP guardrails for data quality
├── .env                   # Local database credentials (ignored by git)
├── main.py                # The single entry point to run the pipeline
├── report.sql             # Schema and analytics queries
└── requirements.txt
```

## 🚀 How to Run Locally

**1. Clone the repository**
```bash
git clone https://github.com/yourusername/insurance-etl-pipeline.git
cd insurance-etl-pipeline
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Set up your environment variables**
Create a file named `.env` in the root directory and add your MySQL credentials:
```text
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=insurance_db
```

**4. Add data**
Place any messy insurance data CSV files into the `data/incoming/` folder. 

**5. Run the pipeline**
```bash
python main.py
```

Check the `logs/etl_process.log` file to see exactly how the system handled the data!
