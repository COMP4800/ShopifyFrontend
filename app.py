from flask import Flask

app = Flask(__name__)


# Place holder index
@app.route('/')
def initialize_frontend():
    return {"data": 'Hello world!'}


# Get all orders from Client ID to date
@app.route('/orders/<client_id>')
def get_client_orders(client_id):
    # Query database for result
    # Return result of query in Data
    return {"client_id": client_id, "data": "REPLACE ME"}


# Get all orders from a specified year of Client ID to date
@app.route('/orders/<client_id>/<year>')
def get_client_orders_year(client_id, year):
    # Query database for result
    # Return result of query in Data
    return {"client_id": client_id,
            "year": year,
            "data": "REPLACE ME"}


# Get all orders from a specified year and month of Client ID to date
@app.route('/orders/<client_id>/<year>/<month>')
def retrieve_client_orders_year_month(client_id, year, month):
    # Query database for result
    # Return result of query in Data
    return {"client_id": client_id,
            "year": year,
            "month": month,
            "data": "REPLACE ME"}
