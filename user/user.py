from flask import Flask, request, jsonify
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
import mysql.connector
import re
import logging
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

@app.route('/v1/user/get_user', methods=['POST'])
def get_user():
    data = request.get_json()
    email = data.get('email')

    if not email:
        return jsonify({'error': 'Email is required'}), 400

    try:
        with get_db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT email, name, phone, rrn, user_id FROM Users WHERE email = %s", (email,))
                user = cursor.fetchone()

        if user:
            return jsonify(user), 200
        else:
            return jsonify({'error': 'User not found'}), 404
    except mysql.connector.Error as err:
        logging.error('MySQL error during get_user: %s', str(err))
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/v1/user/register', methods=['POST'])
def register():
    data = request.get_json()

    name = data.get('name')
    rrn = data.get('rrn')
    phone = data.get('phone')
    email = data.get('email')
    password = data.get('password')

    if not all([name, rrn, phone, email, password]):
        logging.warning('Missing required fields in registration request')
        return jsonify({'error': 'Missing required fields'}), 400

    if not re.match(r'^[0-9]{6}-[0-9]{7}$', rrn):
        logging.warning('Invalid RRN format: %s', rrn)
        return jsonify({'error': 'Invalid RRN format'}), 400

    if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email):
        logging.warning('Invalid email format: %s', email)
        return jsonify({'error': 'Invalid email format'}), 400

    hashed_password = generate_password_hash(password, method='pbkdf2:sha256')

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                INSERT INTO Users (name, rrn, phone, email, password_hash)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(query, (name, rrn, phone, email, hashed_password))
                conn.commit()

                user_id = cursor.lastrowid

        return jsonify({'user_id': user_id}), 201
    except mysql.connector.Error as err:
        logging.error('MySQL error during registration: %s', str(err))
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/v1/user/login', methods=['POST'])
def login():
    data = request.get_json()

    email = data.get('email')
    password = data.get('password')

    if not all([email, password]):
        logging.warning('Missing required fields in login request')
        return jsonify({'error': 'Missing required fields'}), 400

    try:
        with get_db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM Users WHERE email = %s", (email,))
                user = cursor.fetchone()

        if user and check_password_hash(user['password_hash'], password):
            return jsonify({
                'user_id': user['user_id'],
                'name': user['name'],
                'rrn': user['rrn'],
                'phone': user['phone']
            }), 200
        else:
            logging.warning('Invalid login attempt for email: %s', email)
            return jsonify({'error': 'Invalid email or password'}), 401
    except mysql.connector.Error as err:
        logging.error('MySQL error during login: %s', str(err))
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/v1/user/delete', methods=['DELETE'])
def delete_account():
    data = request.get_json()

    email = data.get('email')
    password = data.get('password')

    if not all([email, password]):
        logging.warning('Missing required fields in delete request')
        return jsonify({'error': 'Missing required fields'}), 400

    try:
        with get_db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM Users WHERE email = %s", (email,))
                user = cursor.fetchone()

                if user and check_password_hash(user['password_hash'], password):
                    cursor.execute("DELETE FROM Users WHERE email = %s", (email,))
                    conn.commit()
                    return jsonify({'message': 'User deleted successfully'}), 200
                else:
                    logging.warning('Invalid delete attempt for email: %s', email)
                    return jsonify({'error': 'Invalid email or password'}), 401
    except mysql.connector.Error as err:
        logging.error('MySQL error during user deletion: %s', str(err))
        return jsonify({'error': 'Internal server error'}), 500

@app.route("/v1/user/healthcheck", methods=["GET"])
def healthcheck():
    now = datetime.now()
    return jsonify({
        "msg": "healthy",
        "service": "user",
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    })

if __name__ == '__main__':
    try:
        with get_db_connection() as conn:
            logging.info("Database connection successful.")
        app.run(host='0.0.0.0', debug=True, port=5000)
    except mysql.connector.Error as err:
        logging.error('MySQL error during startup: %s', str(err))
