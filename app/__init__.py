# app/__init__.py
# Author: Shakthi Balasubramanian

from flask import Flask

def create_app():
    app = Flask(__name__)
    app.secret_key = "riskguard-secret-key"

    from app.routes import main
    app.register_blueprint(main)

    return app