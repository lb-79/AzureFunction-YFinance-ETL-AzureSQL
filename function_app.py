import logging
import azure.functions as func
import pyodbc
import os
import datetime
import yfinance as yf

app = func.FunctionApp()

# orario di chiusura del NYSE
# Test every 10 minutes --> "0 */10 * * * *"
# Normal Schedule       --> "0 0 2 * * *"
@app.timer_trigger(schedule="0 0 2 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def timer_trigger_etl(myTimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function started.')

    # Recupera connection string da environment variable
    connection_string = os.environ["AZURE_SQL_CONNECTIONSTRING"]

    # Recupera i dati degli ultimi 7 giorni
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=7)

    # Gruppo 1 - Azioni / ETF
    equity_tickers = ["HMEM.MI", "IUSA.AS", "MJP.PA", "SGLD.MI", "VAPX.AS", "VEUR.AS", "VT"]
    
    # Gruppo 2 - Valute
    fx_tickers = ["EURUSD=X", "USDCHF=X", "EURCHF=X"]  # yfinance usa il suffisso =X per valute

    logging.info("Starting equity fetch")
    fetch_and_store(connection_string,equity_tickers, 'staging.EquityData',start_date,end_date)
    logging.info("Starting FX fetch")
    fetch_and_store(connection_string,fx_tickers, 'staging.FxData',start_date,end_date)
    logging.info("Starting KPI computation")
    computeKPI(connection_string)

#funzione di scrittura su database
def fetch_and_store(connection_string, tickers, table_name,start_date,end_date):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                for ticker in tickers:
                    data = yf.Ticker(ticker).history(start=start_date, end=end_date)
                    if data.empty:
                        continue

                    for idx, row in data.iterrows():
                        cursor.execute(f"""
                            INSERT INTO {table_name} 
                            ([Ticker], [Date], [Open], [High], [Low], [Close], [Volume])
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, ticker, idx.date(), row['Open'], row['High'], row['Low'], row['Close'], row.get('Volume', 0))

    except Exception as e:
        logging.error(f"Error while inserting Data: {e}")

#Chiamata stored procedure per calcolo KPI
def computeKPI(connection_string):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    stored_procedure = "etl.usp_main" 

    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                        cursor.execute("{CALL " + stored_procedure + "}")

    except Exception as e:
        logging.error(f"Error computing KPI: {e}")

