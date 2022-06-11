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
          TABLE_UUID
        FROM 
          `hsbc-9093058-rwapc51-dev.CATALOGUE.VM_METADATA` CROSS JOIN UNNEST(METADATA) AS A
           WHERE FILE_TYPE like 'fotc_rd_cost_centre_to_cg%' 
           AND (A.ATTRIBUTE = 'location' AND  A.VALUE = 'GBQ')
           ORDER BY CREATED DESC LIMIT 1
        """
    )

    # Handle query_job result and return to flask to display
    res = query_job.result()
    tableName = ""
    for row in res:
        tableName = str(row)

    finalTableName = "hsbc-9093058-rwapc51-dev." + tableName
    sqlQuery = """
    select * from `{0}`
    """.format(finalTableName)
    query_job = bigquery_client.query(sqlQuery)
    finalResponse = query_job.result()
    data = []
    for row in res:
        dict = {}
        dict["__uuid"] = str(row[0])
        dict["Cost_Centre"] = str(row[1])
        dict["Customer_Group"] = str(row[2])
        dict["Customer_subgroup_Level_1"] = str(row[3])
        dict["Customer_subgroup_Level_2"] = str(row[4])
        data.append(dict)
    finalData = json.dumps(data)
    pandas.read_json(finalData).to_excel("output.xlsx")
    return "Success!!"

if __name__ == "__main__":
    app.run(port=8080)
