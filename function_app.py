import logging
import azure.functions as func
import pyodbc
import os

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */10 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def timer_trigger_etl(myTimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function started.')

    # Recupera connection string da environment variable
    connection_string = os.environ["AZURE_SQL_CONNECTIONSTRING"]

    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO azure.functionAppHeartBeat DEFAULT VALUES")
                conn.commit()
                logging.info("Heartbeat record inserted successfully.")
    except Exception as e:
        logging.error(f"Error while inserting heartbeat: {e}")
