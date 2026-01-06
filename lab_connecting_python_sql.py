"""
LAB | Connecting Python to SQL (Sakila)
Author: Paulina (comments written in first-person so anyone can follow my logic)

Learning goal:
- Connect Python to a SQL database (MySQL Sakila)
- Run SQL queries from Python
- Work with the results as pandas DataFrames
- Identify customers active in both May and June, and compare their activity

Lab requirements:
1) Create a connection engine to Sakila
2) rentals_month(engine, month, year) -> returns rentals for the selected month/year as a DataFrame
3) rental_count_month(df_rentals, month, year) -> returns rental counts per customer with a dynamic column name
4) compare_rentals(df_counts_1, df_counts_2) -> merges both months and adds a 'difference' column

How to run:
- Update the credentials in the __main__ block
- Run the file: python lab_connecting_python_sql.py
"""

from __future__ import annotations

import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus


# -----------------------------------------------------------------------------
# 0) Connection helper
# -----------------------------------------------------------------------------
def get_engine(
    user: str,
    password: str,
    host: str = "127.0.0.1",
    port: int = 3306,
    database: str = "sakila",
) -> Engine:
    """
    Build a SQLAlchemy engine that Python can use to talk to MySQL.

    Notes:
    - The engine is not the data itself. It's the "connection factory".
    - pandas can use it to run SQL and return DataFrames.

    Important:
    - If the password contains special characters (like @, :, /), I URL-encode it.
    """
    safe_password = quote_plus(password)
    connection_string = f"mysql+pymysql://{user}:{safe_password}@{host}:{port}/{database}"
    return create_engine(connection_string)


# -----------------------------------------------------------------------------
# 1) rentals_month(engine, month, year)
# -----------------------------------------------------------------------------
def rentals_month(engine: Engine, month: int, year: int) -> pd.DataFrame:
    """
    Retrieve rental data for a given month and year from the rental table
    and return it as a pandas DataFrame.

    Parameters:
    - engine: SQLAlchemy engine connected to sakila
    - month: integer month (e.g., 5 for May, 6 for June)
    - year: integer year (e.g., 2005)

    Returns:
    - DataFrame with rental_id, rental_date, customer_id
    """
    query = text("""
        SELECT
            rental_id,
            rental_date,
            customer_id
        FROM rental
        WHERE YEAR(rental_date) = :year
          AND MONTH(rental_date) = :month
        ORDER BY rental_date;
    """)

    df = pd.read_sql(query, engine, params={"year": year, "month": month})
    return df


# -----------------------------------------------------------------------------
# 2) rental_count_month(df_rentals, month, year)
# -----------------------------------------------------------------------------
def rental_count_month(df_rentals: pd.DataFrame, month: int, year: int) -> pd.DataFrame:
    """
    Take the rentals DataFrame (from rentals_month) and compute the number
    of rentals per customer_id for that month/year.

    Requirement from the lab:
    - The output column name must be formatted like rentals_MM_YYYY
      Example: month=5, year=2005  -> rentals_05_2005

    Parameters:
    - df_rentals: DataFrame produced by rentals_month
    - month: integer month
    - year: integer year

    Returns:
    - DataFrame with columns: customer_id, rentals_MM_YYYY
    """
    # Format the month with 2 digits (05 instead of 5) as required  by the lab
    month_str = f"{month:02d}"
    col_name = f"rentals_{month_str}_{year}"

    # groupby customer_id and count the number of rental_id rows
    df_counts = (
        df_rentals
        .groupby("customer_id", as_index=False)["rental_id"]
        .count()
        .rename(columns={"rental_id": col_name})
    )

    return df_counts


# -----------------------------------------------------------------------------
# 3) compare_rentals(df_counts_1, df_counts_2)
# -----------------------------------------------------------------------------
def compare_rentals(df_counts_1: pd.DataFrame, df_counts_2: pd.DataFrame) -> pd.DataFrame:
    """
    Compare rentals between two different months.

    Inputs:
    - df_counts_1: DataFrame with customer_id + rentals_MM_YYYY for month 1
    - df_counts_2: DataFrame with customer_id + rentals_MM_YYYY for month 2

    Output:
    - Combined DataFrame with:
        customer_id
        rentals_MM_YYYY (month 1)
        rentals_MM_YYYY (month 2)
        difference  = (month 2) - (month 1)

    Lab requirement:
    - The customers must be those active in BOTH months.
      Use an INNER JOIN on customer_id (keeps only the intersection).
    """
    # Detect the rentals columns automatically (they start with "rentals_")
    rentals_cols_1 = [c for c in df_counts_1.columns if c.startswith("rentals_")]
    rentals_cols_2 = [c for c in df_counts_2.columns if c.startswith("rentals_")]

    if len(rentals_cols_1) != 1 or len(rentals_cols_2) != 1:
        raise ValueError(
            "Each input DataFrame must have exactly one rentals_* column "
            "(example: rentals_05_2005)."
        )

    col_1 = rentals_cols_1[0]
    col_2 = rentals_cols_2[0]

    # INNER JOIN keeps only customers present in both months (active in both months)
    df_merged = df_counts_1.merge(df_counts_2, on="customer_id", how="inner")

    # difference = second month - first month
    df_merged["difference"] = df_merged[col_2] - df_merged[col_1]

    return df_merged


# -----------------------------------------------------------------------------
# 4) Example "main" workflow (Full solution for May vs June 2005)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Update these values to match my MySQL setup.
    # -------------------------------------------------------------------------
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("DB_HOST", "127.0.0.1")
    PORT = int(os.getenv("DB_PORT", 3306))
    DATABASE = os.getenv("DB_NAME", "sakila")

    engine = get_engine(USER, PASSWORD, HOST, PORT, DATABASE)

    # -------------------------------------------------------------------------
    # Quick connectivity check 
    # -------------------------------------------------------------------------
    df_test = pd.read_sql("SELECT COUNT(*) AS rentals FROM rental;", engine)
    print("\nConnection test:")
    print(df_test)

    # -------------------------------------------------------------------------
    # Step 1: Pull rentals for May and June 2005
    # -------------------------------------------------------------------------
    df_may_rentals = rentals_month(engine, month=5, year=2005)
    df_june_rentals = rentals_month(engine, month=6, year=2005)

    print("\nRentals rows fetched:")
    print(f"May 2005 rentals rows:  {len(df_may_rentals)}")
    print(f"June 2005 rentals rows: {len(df_june_rentals)}")

    # -------------------------------------------------------------------------
    # Step 2: Convert rentals into counts per customer
    # -------------------------------------------------------------------------
    df_may_counts = rental_count_month(df_may_rentals, month=5, year=2005)
    df_june_counts = rental_count_month(df_june_rentals, month=6, year=2005)

    print("\nCustomers with rentals (counts table size):")
    print(f"May 2005 active customers:  {len(df_may_counts)}")
    print(f"June 2005 active customers: {len(df_june_counts)}")

    # -------------------------------------------------------------------------
    # Step 3: Compare customers active in BOTH months + compute difference
    # -------------------------------------------------------------------------
    df_comparison = compare_rentals(df_may_counts, df_june_counts)

    # The biggest increase appears first
    df_comparison = df_comparison.sort_values("difference", ascending=False)

    print("\nCustomers active in BOTH May and June 2005 (top 20 changes):")
    print(df_comparison.head(20))

    # -------------------------------------------------------------------------
    # Save results to CSV: comparison of both months
    # -------------------------------------------------------------------------
    df_comparison.to_csv("customer_rentals_may_vs_june_2005.csv", index=False)
    print("\nSaved: customer_rentals_may_vs_june_2005.csv")
    
    # -------------------------------------------------------------------------
    # 5)  Save results to CSV: 
    # -------------------------------------------------------------------------

    # Individual rentals and counts for May and June 2005

    df_may_counts.to_csv(
    "customer_rentals_may_2005.csv",
    index=False
   )

    df_june_counts.to_csv(
    "customer_rentals_june_2005.csv",
    index=False
  )

    # Individual rentals for May and June 2005 (raw data)

    df_may_rentals.to_csv(
    "rentals_raw_may_2005.csv",
    index=False
  )

    df_june_rentals.to_csv(
    "rentals_raw_june_2005.csv",
    index=False
 )

  # Summary metrics

    summary = pd.DataFrame({
    "metric": [
        "total_rentals",
        "may_rentals",
        "june_rentals",
        "may_active_customers",
        "june_active_customers",
        "active_both_months"
    ],
    "value": [
        int(df_test["rentals"].iloc[0]),
        len(df_may_rentals),
        len(df_june_rentals),
        len(df_may_counts),
        len(df_june_counts),
        len(df_comparison)
    ]
  })

    summary.to_csv("summary_metrics.csv", index=False)
    print("Saved: customer_rentals_may_2005.csv")
    print("Saved: customer_rentals_june_2005.csv")
    print("Saved: rentals_raw_may_2005.csv")
    print("Saved: rentals_raw_june_2005.csv")
    print("Saved: summary_metrics.csv")