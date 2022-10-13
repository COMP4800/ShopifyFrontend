import requests

# Testing URL
# Open a terminal and run wsgi.py
# Open a second terminal and run test.py to tests response from app.py routes
BASE_URL = "http://127.0.0.1:5000/"

response = requests.get(BASE_URL + "orders/1")
# print(response.json())

response = requests.get(BASE_URL + "orders/1/2022")
print(response.json())

response = requests.get(BASE_URL + "orders/1/2020/01")
# print(response.json())
