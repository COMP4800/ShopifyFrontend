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


def dump_table(table_name):
    results = []
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = dynamodb_client.scan(
                TableName=table_name,
                ExclusiveStartKey=last_evaluated_key
            )
        else:
            response = dynamodb_client.scan(TableName=table_name)
        last_evaluated_key = response.get('LastEvaluatedKey')

        results.extend(response['Items'])

        if not last_evaluated_key:
            break
    return results


def get_items_from_db(table_name):
    """
    Get all the orders from AWS DynamoDb
    :param table_name: a string
    :return: A JSON response
    """
    try:
        table = dynamodb.Table(table_name)
        response = table.scan()
        data = response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
        return data
    except ClientError as err:
        return err

    # try:
    #     response = table.scan()['Items']
    #     print(response)
    # except ClientError as e:
    #     print(f"error: {e}")
    # else:
    #     return response


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


@app.route("/")
def welcome():
    return {"Message": "welcome to the EightX API"}


@app.route('/orders')
def initialize_frontend():
    """
    Route for getting all the orders
    """
    # return dump_table("keep-it-wild-az")
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


@app.route('/orders/<year>/<month>')
def get_monthly_orders(year, month):
    """
    This route gets all the orders by month for a single year
    :param year: a number
    :param month: a number
    :return: a JSON
    """
    # table_name = "keep-it-wild-az"
    # results = []
    # last_evaluated_key = None
    # parsed_data = []
    # while True:
    #     if last_evaluated_key:
    #         response = dynamodb_client.scan(
    #             TableName=table_name,
    #             ExclusiveStartKey=last_evaluated_key
    #         )
    #     else:
    #         response = dynamodb_client.scan(TableName=table_name)
    #     last_evaluated_key = response.get('LastEvaluatedKey')
    #
    #     results.extend(response['Items'])
    #     print(len(results))
    #
    #     for each_item in response['Items']:
    #         # print(each_item["OrderDate"])
    #         if len(each_item["OrderDate"]) == 20:
    #             date = datetime.datetime.fromisoformat(each_item["OrderDate"].rstrip(each_item["OrderDate"][-1]))
    #             if date.year == int(year) and date.month == int(month):
    #                 parsed_data.append(each_item)
    #         else:
    #             print("no")
    #
    #     if not last_evaluated_key:
    #         break
    # return parsed_data


    try:
        table = dynamodb.Table("test1")
        response = table.scan(
            # FilterExpression=filter_expression
        )
        data = response['Items']
        parsed_data = []
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
            print(len(data))
            for each_item in response['Items']:
                date_year = str(each_item["OrderDate"])[0:4]
                date_month = str(each_item["OrderDate"])[5:7]
                if date_year == year and date_month == month:
                # date = datetime.datetime.fromisoformat(each_item["OrderDate"].rstrip(each_item["OrderDate"][-1]))
                # if date.year == int(year) and date.month == int(month):
                    parsed_data.append(each_item)

        return parsed_data
    except ClientError as err:
        return {"Error": f"{err}"}


@app.route('/orders/<year>')
def get_yearly_orders(year):
    """
    This route gets all the orders by year
    :param year: an int
    :return: a JSON
    """

    try:
        filter_expression = Key('OrderDate').between(f'{year}-01-01T00:00:00Z', f'{year}-12-31T24:00:00Z')
        table = dynamodb.Table("keep-it-wild-az")
        response = table.scan(
            # FilterExpression=filter_expression
        )
        data = response['Items']
        parsed_data = []
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
            print(len(data))
            for each_item in response['Items']:
                date = datetime.datetime.fromisoformat(each_item["OrderDate"].rstrip(each_item["OrderDate"][-1]))
                if date.year == int(year):
                    parsed_data.append(each_item)
        print(len(parsed_data))
        return parsed_data
    except ClientError as err:
        return err

    # response = table.scan(
    # )
    # return response['Items']


# @app.route('/orders/<start_date>/<end_date>')
# def get_order_between_date_range(start_date, end_date):
#     try:
#         filter_expression = Key('OrderDate').between(f'{start_date}T00:00:00Z', f'{end_date}T24:00:00Z')
#         table = dynamodb.Table("keep-it-wild-az")
#         response = table.scan(
#             FilterExpression=filter_expression
#         )
#         data = response['Items']
#         while 'LastEvaluatedKey' in response:
#             response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
#             data.extend(response['Items'])
#         return data
#     except ClientError as err:
#         return err

    # response = table.query(
    #     IndexName="OrderID-OrderDate-index",
    #     # KeyConditionExpression=Key('OrderDate').between(f'{start_date}T12:00:00Z', f'{end_date}T12:00:00Z'),
    #     KeyCondition=f"OrderID = :id and OrderDate = :{start_date} BETWEEN {end_date}"
    #
    #     # FilterExpression=f'OrderDate = :OrderDate between :{start_date} and :{end_date}'
    #
    #
    #     # KeyConditionExpression=Key('OrderDate').lt(start_date)
    # )
    # return response['Items']
