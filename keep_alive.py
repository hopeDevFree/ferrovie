import os

from flask import Flask
from threading import Thread

app = Flask('')


@app.route('/')
def home():
    return "Script is running!"


def run():
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 4000)))


def keep_alive():
    t = Thread(target=run)
    t.start()
