import requests

# Testing URL
# Open a terminal and run wsgi.py
# Open a second terminal and run test.py to tests response from app.py routes
BASE_URL = "http://127.0.0.1:5000/"

response = requests.get(BASE_URL + "orders/12345")
print(response.json())

response = requests.get(BASE_URL + "orders/12345/2020")
print(response.json())

response = requests.get(BASE_URL + "orders/12345/2020/01")
print(response.json())
