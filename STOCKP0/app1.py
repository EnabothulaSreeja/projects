import mysql.connector
import pandas as pd
from config import DB_CONFIG
from visualizations import plot_stock_price

def connect_db():
    return mysql.connector.connect(**DB_CONFIG)

def create_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stocks (
        id INT AUTO_INCREMENT PRIMARY KEY, 
        date DATE,
        symbol VARCHAR(10),
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT
    )
    """)

def insert_data(cursor, data):
    for _, row in data.iterrows():
        cursor.execute("""
            INSERT INTO stocks (date, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

def get_stock_data(symbol, start_date, end_date, connection):
    query = """
        SELECT date, close 
        FROM stocks 
        WHERE symbol = %s AND date BETWEEN %s AND %s 
        ORDER BY date
    """
    return pd.read_sql(query, connection, params=(symbol, start_date, end_date))
def get_stock_details_by_date(date, connection):
    query = """
        SELECT date, symbol, high, low, volume
        FROM stocks
        WHERE date = %s
        ORDER BY symbol
    """
    return pd.read_sql(query, connection, params=(date,))


if __name__ == "__main__":
    conn = connect_db()
    cursor = conn.cursor()

    create_table(cursor)
    df = pd.read_csv("stock_data.csv")  # Path to uploaded file
    insert_data(cursor, df)
    conn.commit()

    # --- User Input ---
    symbol = input("Enter stock symbol to visualize (e.g., AAPL): ").upper()
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")

    # --- Fetch + Visualize ---
    df_stock = get_stock_data(symbol, start_date, end_date, conn)
    if not df_stock.empty:
        plot_stock_price(df_stock, symbol, start_date, end_date)
    else:
        print("No data found for that symbol and date range.")
        
    date_input = input("Enter a specific date to display all stock details (YYYY-MM-DD): ")
    df_details = get_stock_details_by_date(date_input, conn)

    if not df_details.empty:
        print(f"\nStock Details on {date_input}:\n")
        print(df_details.to_string(index=False))
    else:
        print("No stock data found for that date.")

    cursor.close()
    conn.close()
