#organization name and keycloak credentials are hardcoded so fill the line 30, 212 and 242
import logging 
from sys import stdout
from flask import Flask, request, redirect, url_for, session, render_template
from keycloak import KeycloakOpenID
from minio import Minio
import pytz, requests
import xml.etree.ElementTree as ElementTree
from trino.dbapi import connect
from trino.auth import BasicAuthentication, JWTAuthentication

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Chiave segreta per la sessione
logger = logging.getLogger('mylogger')
TRINO_HOST="https://host.docker.internal"
TRINO_PORT="8443"
TRINO_SCHEME="https"
keycloak_url = "http://host.docker.internal:8080/authrealms/master/protocol/openid-connect/token"


timezone = pytz.timezone("UTC")

logger.setLevel(logging.DEBUG)
logFormatter = logging.Formatter("%(asctime)s  %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout) #set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

# Configurazione di Keycloak
keycloak_openid = KeycloakOpenID(server_url="http://host.docker.internal:8080/auth/",
                                 client_id="",
                                 realm_name="",
                                 client_secret_key="")


@app.route('/')
def home():
    if 'token' in session:
        # L'utente è autenticato, mostra la home page
        return render_template('home.html')
    else:
        # Reindirizza l'utente a Keycloak per il login
        auth_url = keycloak_openid.auth_url(
            redirect_uri="http://localhost:5000/login/callback",
            scope="email",
            # state="your_state_info",
        )
        return redirect(auth_url)
    
@app.route('/unathorized')
def fail_login():
    return render_template('fail.html')

@app.route('/login/callback')
def login_callback():
    code = request.args.get('code')
    token = keycloak_openid.token(
        grant_type='authorization_code',
        code=code,
        redirect_uri="http://localhost:5000/login/callback"
    )
    logger.info("token"+token['access_token'])
    if token:
        # Login riuscito, memorizza il token nella sessione
        session['token'] = token
        return redirect(url_for('home'))
    else:
        # Login fallito, reindirizza all'home page
        return redirect(url_for('fail_login'))

@app.route('/logout')
def logout():
    session.pop('token', None)
    return redirect(url_for('home'))

@app.route('/sqla')
def execute_sql_on_trino():
        """Generic function to execute a SQL statement"""
        client = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            http_scheme=TRINO_SCHEME,
            auth=JWTAuthentication(session['token']['access_token']),
            timezone=str(timezone),
            verify=False,
        )


        # Get a cursor from the connection object
        cur = client.cursor()

        sql="SELECT current_user"
        cur.execute(sql)
        
        rows = cur.fetchall()
        logger.info("Current user")
        logger.info(rows)
        
        sql="SELECT current_groups()"
        cur.execute(sql)
        
        rows = cur.fetchall()
        logger.info("Current groups")
        logger.info(rows)

        sql = request.args.get("query")
        logger.info(sql)
        cur.execute(sql)

        # Get the results from the cluster
        rows = cur.fetchall()

        logger.info(rows)
        # Return the results
        return render_template('result.html',response_array=rows)
    
@app.route('/sqlb')
def execute_sql_on_trino1():
        """Generic function to execute a SQL statement"""
        client = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            http_scheme=TRINO_SCHEME,
            auth=JWTAuthentication(session['token']['access_token']),
            timezone=str(timezone),
            verify=False,
        )

        # Get a cursor from the connection object
        cur = client.cursor()

        sql="SELECT current_user"
        cur.execute(sql)
        
        rows = cur.fetchall()
        logger.info("Current user")
        logger.info(rows)
        
        sql="SELECT current_groups()"
        cur.execute(sql)
        
        rows = cur.fetchall()
        logger.info("Current groups")
        logger.info(rows)

        # Execute sql statement
        sql="SELECT * FROM memory.default.tableB"
        cur.execute(sql)

        # Get the results from the cluster
        rows = cur.fetchall()

        logger.info(rows)
        # Return the results
        return render_template('result.html',response_array=rows)

@app.route('/miniobuckets')
def get_minio_file():
       
        # Step 2: Generate MinIO Credentials using the AssumeRoleWithWebIdentity API
        minio_url = "http://host.docker.internal:9000"
        minio_data = {
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "WebIdentityToken": session['token']['access_token']
        }

        response = requests.post(minio_url, data=minio_data)
        xml_data = ElementTree.fromstring(response.text)
    
        # Step 3: Parse the output to extract the credentials
        access_key = xml_data.find('.//{https://sts.amazonaws.com/doc/2011-06-15/}AccessKeyId').text
        secret_access_key = xml_data.find('.//{https://sts.amazonaws.com/doc/2011-06-15/}SecretAccessKey').text
        session_token = xml_data.find('.//{https://sts.amazonaws.com/doc/2011-06-15/}SessionToken').text

        client = Minio(
            endpoint="host.docker.internal:9000",
            access_key=access_key,
            secret_key=secret_access_key,
            session_token=session_token,
            secure=False,
        )
        buckets = client.list_buckets()
        return render_template('result.html',response_array=buckets)
    
@app.route('/minioa')
def get_minio_bucket():
       
        # Step 2: Generate MinIO Credentials using the AssumeRoleWithWebIdentity API
        minio_url = "http://host.docker.internal:9000"
        minio_data = {
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "WebIdentityToken": session['token']['access_token']
        }

        response = requests.post(minio_url, data=minio_data)
        xml_data = ElementTree.fromstring(response.text)
    
        # Step 3: Parse the output to extract the credentials
        access_key = xml_data.find('.//{https://sts.amazonaws.com/doc/2011-06-15/}AccessKeyId').text
        secret_access_key = xml_data.find('.//{https://sts.amazonaws.com/doc/2011-06-15/}SecretAccessKey').text
        session_token = xml_data.find('.//{https://sts.amazonaws.com/doc/2011-06-15/}SessionToken').text

        client = Minio(
            endpoint="host.docker.internal:9000",
            access_key=access_key,
            secret_key=secret_access_key,
            session_token=session_token,
            secure=False,
        )
        objects = client.list_objects("org_name") #replace org_name with organization name
        names = [obj.object_name for obj in objects]
        return render_template('result.html',response_array=names)
    
@app.route('/miniob')
def get_minio_file1():
        # Step 2: Generate MinIO Credentials using the AssumeRoleWithWebIdentity API
        minio_url = "http://host.docker.internal:9000"
        minio_data = {
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "WebIdentityToken": session['token']['access_token']
        }

        response = requests.post(minio_url, data=minio_data)
        logger.info(response.text)
        xml_data = ElementTree.fromstring(response.text)
    
        # Step 3: Parse the output to extract the credentials
        access_key = xml_data.find('.//{https://sts.amazonaws.com/doc/2011-06-15/}AccessKeyId').text
        secret_access_key = xml_data.find('.//{https://sts.amazonaws.com/doc/2011-06-15/}SecretAccessKey').text
        session_token = xml_data.find('.//{https://sts.amazonaws.com/doc/2011-06-15/}SessionToken').text

        client = Minio(
            endpoint="host.docker.internal:9000",
            access_key=access_key,
            secret_key=secret_access_key,
            session_token=session_token,
            secure=False,
        )
        objects = client.list_objects("org_name") #replace org_name with organization name
        names = [obj.object_name for obj in objects]
        return render_template('result.html',response_array=names)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
