from flask import Flask, request, jsonify, abort
from datetime import datetime
import mysql.connector
import random
import logging
import re
import requests
import configparser
import os

app = Flask(__name__)

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app.config['MYSQL_USER'] = os.environ["MYSQL_USER"]
app.config['MYSQL_PASSWORD'] = os.environ["MYSQL_PASS"]
app.config['MYSQL_DATABASE'] = os.environ["MYSQL_DBNAME"]
app.config['MYSQL_HOST'] = os.environ["MYSQL_HOST"]
app.config['MYSQL_PORT'] = os.environ["MYSQL_PORT"]

def get_db_connection():
    return mysql.connector.connect(
        user=app.config['MYSQL_USER'],
        password=app.config['MYSQL_PASSWORD'],
        host=app.config['MYSQL_HOST'],
        database=app.config['MYSQL_DATABASE'],
        port=app.config['MYSQL_PORT']
    )

def generate_account_id():
    prefix = "2000"
    middle = str(random.randint(100, 999)).zfill(3)
    suffix = str(random.randint(1000, 9999)).zfill(4)
    return f"{prefix}-{middle}-{suffix}"

def get_user(email):
    response = requests.post('http://user.skillsbank.local:5000/v1/user/get_user', json={'email': email})
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Failed to get user for email {email}: {response.status_code}")
        return None

def get_account(email):
    try:
        with get_db_connection() as db_connection:
            cursor = db_connection.cursor()
            cursor.execute("SELECT account_id, balance FROM Users INNER JOIN Accounts ON Users.user_id = Accounts.user_id WHERE email = %s", (email,))
            result = cursor.fetchone()

            if result is None:
                return jsonify({"error": "Account not found"}), 404

            return jsonify({"account_id": result[0], "balance": result[1]})
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return jsonify({"error": "Internal server error"}), 500

def create_account(email):
    account_id = generate_account_id()
    user = get_user(email)

    if not user:
        return jsonify({"error": "Email not found"}), 404

    try:
        with get_db_connection() as db_connection:
            cursor = db_connection.cursor()
            cursor.execute("SELECT account_id FROM Accounts WHERE user_id = %s", (user['user_id'],))
            account_holding = cursor.fetchone()

            if account_holding is not None:
                return jsonify({"error": "Account already exists"}), 400

            cursor.execute("INSERT INTO Accounts (user_id, account_id, balance) VALUES (%s, %s, 0)", (user['user_id'], account_id))
            db_connection.commit()

            return jsonify({"msg": "Account issuance has been completed.", "account_id": account_id, "balance": 0})
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return jsonify({"error": "Internal server error"}), 500

def delete_account(email):
    user = get_user(email)

    if not user:
        return jsonify({"error": "Email not found"}), 404

    try:
        with get_db_connection() as db_connection:
            cursor = db_connection.cursor()

            cursor.execute("DELETE FROM Accounts WHERE user_id = %s", (user['user_id'],))
            db_connection.commit()

            return jsonify({"msg": "Account deletion has been completed."})
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/v1/account/create_account', methods=['POST'])
def create_account_endpoint():
    data = request.json
    email = data.get('email')

    if not email or not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email):
        logging.warning('Invalid email format: %s', email)
        return jsonify({'error': 'Invalid email format'}), 400

    return create_account(email)

@app.route('/v1/account/delete_account', methods=['DELETE'])
def delete_account_endpoint():
    data = request.json
    email = data.get('email')

    if not email or not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email):
        logging.warning('Invalid email format: %s', email)
        return jsonify({'error': 'Invalid email format'}), 400

    return delete_account(email)

@app.route('/v1/account/get_account', methods=['POST'])
def get_account_endpoint():
    data = request.json
    email = data.get('email')

    if not email or not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email):
        logging.warning('Invalid email format: %s', email)
        return jsonify({'error': 'Invalid email format'}), 400

    return get_account(email)

@app.route("/v1/account/healthcheck", methods=["GET"])
def healthcheck():
    now = datetime.now()
    data = {
        "msg": "healthy",
        "service": "account",
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    }
    return jsonify(data)

if __name__ == '__main__':
    try:
        with get_db_connection() as conn:
            logging.info("Database connection successful.")
        app.run(host='0.0.0.0', debug=True, port=5000)
    except mysql.connector.Error as err:
        logging.error(f'MySQL error during login: {err}')
