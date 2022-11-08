from flask import Flask
from botocore.exceptions import ClientError
import boto3
import os
from boto3.dynamodb.conditions import Key

app = Flask(__name__)
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


@app.route('/orders/<year>/<month>/<day>')
def get_daily_orders(year, month, day):
    try:
        # table = dynamodb.Table("keep-it-wild-az")
        table = dynamodb.Table("test1")
        response = table.scan(
            # FilterExpression=filter_expression
        )
        data = response['Items']
        parsed_data = []
        for each_item in response['Items']:
            date_year = str(each_item["OrderDate"])[0:4]
            date_month = str(each_item["OrderDate"])[5:7]
            date_day = str(each_item["OrderDate"])[8:10]
            if date_year == year and date_month == month and date_day == day:
                # date = datetime.datetime.fromisoformat(each_item["OrderDate"].rstrip(each_item["OrderDate"][-1]))
                # if date.year == int(year) and date.month == int(month):
                parsed_data.append(each_item)
        return parsed_data
    except ClientError as err:
        return {"err": err}


@app.route('/orders/<year>/<month>')
def get_monthly_orders(year, month):
    """
    This route gets all the orders by month for a single year
    :param year: a number
    :param month: a number
    :return: a JSON
    """
    try:
        # table = dynamodb.Table("keep-it-wild-az")
        table = dynamodb.Table("test2")
        response = table.scan(
            # FilterExpression=filter_expression
        )
        data = response['Items']
        parsed_data = []
        net_sales = 0
        total_sales = 0
        for each_item in response['Items']:
            date_year = str(each_item["OrderDate"])[0:4]
            date_month = str(each_item["OrderDate"])[5:7]
            if date_year == year and date_month == month:
                net_sales += float(each_item["NetSales"])
                total_sales += float(each_item["TotalSales"])
                # date = datetime.datetime.fromisoformat(each_item["OrderDate"].rstrip(each_item["OrderDate"][-1]))
                # if date.year == int(year) and date.month == int(month):
                parsed_data.append(each_item)
        print(f"TotalSales: {total_sales}")
        print(f"NetSales: {net_sales}")
        # while 'LastEvaluatedKey' in response:
        #     response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        #     data.extend(response['Items'])
        #     print(len(data))
        #     for each_item in response['Items']:
        #         date_year = str(each_item["OrderDate"])[0:4]
        #         date_month = str(each_item["OrderDate"])[5:7]
        #         if date_year == year and date_month == month:
        #         # date = datetime.datetime.fromisoformat(each_item["OrderDate"].rstrip(each_item["OrderDate"][-1]))
        #         # if date.year == int(year) and date.month == int(month):
        #             parsed_data.append(each_item)

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
        table = dynamodb.Table("test2")
        response = table.scan(
            # FilterExpression=filter_expression
        )
        data = response['Items']
        parsed_data = []
        item_count = 0
        gross_sales = 0
        net_sales = 0
        total_sales = 0
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
            print(len(data))
            item_count += len(response["Items"])
            for each_item in response['Items']:
                gross_sales += float(each_item["GrossSales"])
                net_sales += float(each_item["NetSales"])
                total_sales += float(each_item["TotalSales"])
                date_year = str(each_item["OrderDate"])[0:4]
                if date_year == str(year):
                    parsed_data.append(each_item)
        print(f'Item Count: {item_count}')
        print(len(parsed_data))
        print(gross_sales)
        print(net_sales)
        print(total_sales)
        return parsed_data
    except ClientError as err:
        return err


# @app.route('/<client_name>/orders/<year>/<month>')
# def get_orders_by_month(client_name, year, month):
#     table = dynamodb.Table(client_name)
#     lastEvaluatedKey = None
#     items = []
#     parsed_items = []
#     while True:
#         if lastEvaluatedKey is None:
#             response = table.scan()
#         else:
#             response = table.scan(
#                 ExclusiveStartKey=lastEvaluatedKey
#             )
#         items.extend(response['Items'])
#         print(len(items))
#         for each_item in response['Items']:
#             if str(each_item["OrderDate"])[0:4] == year:
#                 parsed_items.append(each_item)
#         if 'LastEvaluatedKey' in response:
#             lastEvaluatedKey = response['LastEvaluatedKey']
#         else:
#             break
#     print(len(items))
#     print(len(parsed_items))
#     return parsed_items


@app.route('/<client_name>/orders/<year>/<month>')
def get_orders_by_month_using_lsi(client_name, year, month):
    """QUERY FIX!"""
    """
    Get orders by month from the Local Secondary Index
    :param client_name: name of the client
    :param year: the year -> yyyy
    :param month: the month -> mm
    :return: a list of JSON objects
    """
    print("Hello")
    try:
        table = dynamodb.Table(f'{client_name}-raw')
        Items = []
        response = table.query(
            IndexName="OrdersByMonthAndDate",
            KeyConditionExpression=Key('Year').eq(year) & Key('OrderDate').between(f'{year}-{month}-01',
                                                                                   f'{year}-{int(month) + 1}-01')
        )
        Items.extend(response['Items'])
        print(response)

        while 'LastEvaluatedKey' in response:
            response = table.query(
                IndexName="OrdersByMonthAndDate",
                KeyConditionExpression=Key('Year').eq(year) & Key('OrderDate').between(f'{year}-{month}-01',
                                                                                       f'{year}-{int(month) + 1}-01'),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            Items.extend(response['Items'])
        return Items
    except ClientError as err:
        print(err)
        return {"err": f'{err}'}


@app.route('/<client_name>/orders/<year>/')
def get_orders_by_year_using_lsi(client_name, year):
    """QUERY FIX"""
    """
    Get orders by year from the Local Secondary Index
    :param client_name: name of the client
    :param year: the year -> yyyy
    :return: a list of JSON objects
    """
    print("Hello")
    try:
        table = dynamodb.Table(f'{client_name}-raw')
        Items = []
        response = table.query(
            IndexName="OrdersByMonthAndDate",
            KeyConditionExpression=Key('Year').eq(year)
        )
        Items.extend(response['Items'])
        print(response)

        while 'LastEvaluatedKey' in response:
            response = table.query(
                IndexName="OrdersByMonthAndDate",
                KeyConditionExpression=Key('Year').eq(year),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            Items.extend(response['Items'])
        return Items
    except ClientError as err:
        print(err)
        return {"err": f'{err}'}
