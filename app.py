from flask import Flask

app = Flask(__name__)


@app.route('/')
def initialize_frontend():
    return 'Welcome to the start of the Frontend For COMP 4800'
