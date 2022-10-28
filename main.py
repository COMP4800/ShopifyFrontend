import json
import datetime

import requests
from flask import Flask, request
import re
import pymongo
from botocore.exceptions import ClientError
from bson import json_util
import boto3
import os
from boto3.dynamodb.conditions import Key


app = Flask(__name__)

# MONGO_CONNECTION_URI = "mongodb+srv://adminTester:ETh5oidcvfuVCwWr@testinggrounds.brdchna.mongodb.net/?retryWrites
# =true&w=majority" client = pymongo.MongoClient(MONGO_CONNECTION_URI) DB = client.get_database('EightXTest')
# collection = DB.get_collection('Orders')

dynamodb = boto3.resource(service_name='dynamodb',
                          aws_access_key_id=os.getenv("AccessKey"),
                          aws_secret_access_key=os.getenv("SecretKey"),
                          region_name="ca-central-1")
dynamodb_client = boto3.client(service_name='dynamodb',
                               aws_access_key_id=os.getenv("AccessKey"),
                               aws_secret_access_key=os.getenv("SecretKey"),
                               region_name="ca-central-1")


def get_items_from_db(table_name):
    """
    Get all the orders from AWS DynamoDb
    :param table_name: a string
    :return: A JSON response
    """
    try:
        table = dynamodb.Table(table_name)
        response = table.scan()['Items']
        print(response)
    except ClientError as e:
        print(f"error: {e}")
    else:
        return response

# Pseudo DB
# Db = {
#     "clients": [
#         {"_id": 1}, {"_id": 2}, {"_id": 3}
#     ],
#     "orders": [
#         {
#             "_id": 1,
#             "client_id": 1,
#             "customer_id": 1,
#             "month": "2/31/2021",
#             "orders": 1,
#             "gross_sales": 497,
#             "discounts": -497,
#             "returns": 0,
#             "net_sales": 0,
#             "shipping": 0,
#             "taxes": 0,
#             "total_sales": 0,
#             "average_order_value": 0
#         },
#         {
#             "_id": 2,
#             "client_id": 2,
#             "customer_id": 18,
#             "month": "7/31/2022",
#             "orders": 1,
#             "gross_sales": 497,
#             "discounts": -497,
#             "returns": 0,
#             "net_sales": 0,
#             "shipping": 0,
#             "taxes": 0,
#             "total_sales": 0,
#             "average_order_value": 0
#         },
#         {
#             "_id": 3,
#             "client_id": 1,
#             "customer_id": 17,
#             "month": "7/31/2022",
#             "orders": 1,
#             "gross_sales": 51,
#             "discounts": -7.65,
#             "returns": 0,
#             "net_sales": 43.35,
#             "shipping": 9.99,
#             "taxes": 0,
#             "total_sales": 53.34,
#             "average_order_value": 53.34
#         },
#
#     ],
# }


# Place holder index
@app.route('/')
def initialize_frontend():
    """
    Route for getting all the orders
    """
    return get_items_from_db("keep-it-wild-az")
#
#
# # Get all orders from Client ID to date
# @app.route('/orders/<client_id>')
# def get_client_orders(client_id):
#     # Query database for result
#     orders = collection.find({"shopify_id": client_id})
#     data = json.loads(json_util.dumps(list(orders)))
#     # Return result of query in Data
#     return {"client_id": client_id, "data": data}
#
#
# # Get all orders from a specified year of Client ID to date
# @app.route('/orders/<client_id>/<year>')
# def get_client_orders_year(client_id, year):
#     # Query database for result
#     orders = collection.find({"shopify_id": client_id})
#     data = json.loads(json_util.dumps(list(orders)))
#     parsed_data = []
#     for _each in data:
#         date = _each["date"]
#         if re.search(f"/{year}", f"{date}"):
#             parsed_data.append(_each)
#     # Return result of query in Data
#     return {"client_id": client_id,
#             "year": year,
#             "data": parsed_data}
#
#
# # Get all orders from a specified year and month of Client ID to date
# @app.route('/orders/<client_id>/<year>/<month>')
# def get_client_orders_year_month(client_id, year, month):
#     # Query database for result
#     orders = collection.find({"shopify_id": client_id})
#     data = json.loads(json_util.dumps(list(orders)))
#     parsed_data = []
#     for _each in data:
#         date = _each["date"]
#         if re.search(f"/{year}", f"{date}"):
#             if date[1] == "/":
#                 striped_string = date[0]
#                 print(striped_string)
#                 if int(month) < int(striped_string):
#                     parsed_data.append(_each)
#             else:
#                 striped_string = date[0:2]
#                 print(striped_string)
#                 if int(month) < int(striped_string):
#                     parsed_data.append(_each)
#             # parsed_data.append(_each)
#
#     # Return result of query in Data
#     return {"client_id": client_id,
#             "year": year,
#             "month": month,
#             "data": parsed_data}
#

# @app.route('/orders/<client_id>/transform/<customer_id>')
# def get_client_first_order_date(client_id, customer_id):
#     # Query database for result
#     orders = collection.find({"shopify_id": client_id})
#     data = json.loads(json_util.dumps(list(orders)))
#     parsed_data = []
#     # Return result of query in Data
#     for _each in data:
#         cid = _each["customer_id"]
#         if re.search(f"{customer_id}", f"{cid}"):
#             parsed_data.append(_each.get("date"))
#     dates = [datetime.datetime.strptime(ts, "%m/%d/%Y") for ts in parsed_data]
#     dates.sort()
#     sorted_dates = [datetime.datetime.strftime(ts, "%m/%d/%Y") for ts in dates]
#     first_date = sorted_dates[0]
#     return {"client_id": client_id, "customer_id": customer_id, "first_order_date": first_date}


@app.route('/orders/<year>')
def get_yearly_orders(year):
    """
    This route gets all the orders by year
    :param year: an int
    :return: a JSON
    """
    table = dynamodb.Table("keep-it-wild-az")
    filter_expression = Key('OrderDate').between(f'{year}-01-01', f'{year}-12-31')
    response = table.scan(
        FilterExpression=filter_expression
    )
    return response['Items']


# @app.route('/orders/<start_date>/<end_date>')
# def get_order_between_date_range(start_date, end_date):
#     table = dynamodb.Table("keep-it-wild-az")
#     filter_expression = Key('OrderDate').between(start_date, f'{end_date}T24:00:00Z')
#     response = table.scan(
#         FilterExpression=filter_expression
#     )
#     # response = table.query(
#     #     IndexName="OrderID-OrderDate-index",
#     #     # KeyConditionExpression=Key('OrderDate').between(f'{start_date}T12:00:00Z', f'{end_date}T12:00:00Z'),
#     #     KeyCondition=f"OrderID = :id and OrderDate = :{start_date} BETWEEN {end_date}"
#     #
#     #     # FilterExpression=f'OrderDate = :OrderDate between :{start_date} and :{end_date}'
#     #
#     #
#     #     # KeyConditionExpression=Key('OrderDate').lt(start_date)
#     # )
#     return response['Items']
