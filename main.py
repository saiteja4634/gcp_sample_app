import flask
import os
import json
from google.cloud import bigquery
import pandas
bigquery_client = bigquery.Client()

credential_path = "test-project-12345.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

app = flask.Flask(__name__)

@app.route("/")
def main():
    # Define query_job and query object
    query_job = bigquery_client.query(
        """
        SELECT 
          reactions
        FROM 
          `bigquery-public-data.fda_food.food_events` LIMIT 100
        """
    )

    # Handle query_job result and return to flask to display
    res = query_job.result()
    data = []
    for row in res:
        dict = {}
        dict["reactions"] = str(row[0])
        data.append(dict)
    return json.dumps(data)

@app.route('/get-exel-data')
def get_incomes():
    # Define query_job and query object
    query_job = bigquery_client.query(
        """
        SELECT 
          reactions
        FROM 
          `bigquery-public-data.fda_food.food_events` LIMIT 100
        """
    )

    # Handle query_job result and return to flask to display
    res = query_job.result()
    data = []
    for row in res:
        dict = {}
        dict["reactions"] = str(row[0])
        data.append(dict)
    finalData = json.dumps(data)
    pandas.read_json(finalData).to_excel("output.xlsx")
    return "Success!!"

if __name__ == "__main__":
    app.run(port=8080)
