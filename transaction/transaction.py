from flask import Flask, request, jsonify, abort
from datetime import datetime
import mysql.connector
import logging
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

def get_account(email):
    response = requests.post('http://account.skillsbank.local:5000/v1/account/get_account', json={'email': email})
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Failed to get account for email {email}: {response.status_code}")
        return None

def transfer_func(email, account_id_source, account_id_target, amount):
    source_account = get_account(email)

    if not source_account or source_account.get("account_id") != account_id_source:
        abort(400, description="source account not found or does not match")

    try:
        db_connection = get_db_connection()
        cursor = db_connection.cursor()

        cursor.execute("SELECT balance FROM Accounts WHERE account_id = %s FOR UPDATE", (account_id_source,))
        source_balance = cursor.fetchone()

        if not source_balance:
            abort(400, description="source account not found")

        if float(source_balance[0]) < amount:
            abort(400, description="your balance is insufficient")

        cursor.execute("SELECT account_id FROM Accounts WHERE account_id = %s", (account_id_target,))
        target_account = cursor.fetchone()

        if not target_account:
            abort(400, description="target account not found")

        cursor.execute("UPDATE Accounts SET balance = balance - %s WHERE account_id = %s", (amount, account_id_source))
        cursor.execute("UPDATE Accounts SET balance = balance + %s WHERE account_id = %s", (amount, account_id_target))
        cursor.execute("INSERT INTO Transactions (account_id_from, account_id_to, amount, transaction_type) VALUES (%s, %s, %s, 'transfer')", (account_id_source, account_id_target, amount))
        db_connection.commit()

        return jsonify({"msg": "transfer has been completed"})

    except mysql.connector.Error as err:
        logging.error(f"Error during transfer: {err}")
        db_connection.rollback()
        abort(500, description="internal server error")
    finally:
        cursor.close()
        db_connection.close()

@app.route("/v1/transaction/transfer", methods=["POST"])
def transfer():
    data = request.get_json()

    email = data.get('email')
    account_id_source = data.get('account_id_source')
    account_id_target = data.get('account_id_target')
    amount = data.get('amount')

    if account_id_source == account_id_target:
        abort(400, description="source and target are the same")

    return transfer_func(email, account_id_source, account_id_target, amount)

@app.route("/v1/transaction/list_transaction", methods=["GET"])
def get_transactions():
    data = request.get_json()

    email = data.get('email')
    account_id_source = data.get('account_id_source')
    account_id_target = data.get('account_id_target')
    amount = data.get('amount')

    if account_id_source == account_id_target:
        abort(400, description="source and target are the same")

    return transfer_func(email, account_id_source, account_id_target, amount)

@app.route("/v1/transaction/healthcheck", methods=["GET"])
def healthcheck():
    now = datetime.now()
    data = {
        "msg": "healthy",
        "service": "transaction",
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    }
    return jsonify(data)

if __name__ == '__main__':
    try:
        get_db_connection()
        app.run(host='0.0.0.0', debug=True, port=5000)
    except mysql.connector.Error as err:
        logging.error('MySQL error during login: %s', str(err))
