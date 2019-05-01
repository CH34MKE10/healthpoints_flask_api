from flask import Flask, request, render_template, jsonify
from flask_cors import CORS
from werkzeug.exceptions import HTTPException
import snowflake.connector
from snowflake.connector import DictCursor
import uuid
import os


app = Flask(__name__)
CORS(app)

# utility functions
def get_generated_id():

    id = str(uuid.uuid4())

    return id


def get_snowflake_conn():

    ctx = snowflake.connector.connect(
    user='',
    password='',
    account=''
    )

    ctx.cursor().execute("")
    ctx.cursor().execute("")

    return ctx


def close_snowflake_conn(ctx):

    ctx.close()

    return

'''
On the frontend check the key of the response...

data: is a successful response

error: well not so much

'''

# maybe add some better error handling...
@app.errorhandler(Exception)
def handle_error(e):
    code = 500
    if isinstance(e, HTTPException):
        code = e.code
    return jsonify(error=str(e)), code


@app.route("/")
def home_func():
    # this should check for an authenticated user then route to dashboard if true and login if false.
    return 'Yes it works!'

@app.route("/snowflake_test")
def snowflake_func():

    ctx = snowflake.connector.connect(
    user=user,
    password=password,
    account=account
    )

    cs = ctx.cursor()

    try:
       cs.execute("SELECT current_version()")
       one_row = cs.fetchone()
    finally:
       cs.close()
    ctx.close()

    data = {'data': one_row[0]}

    resp = jsonify(data)
    resp.status_code = 200

    return resp


@app.route("/profile", methods = ['GET', 'POST', 'PUT', 'DELETE'])
def profile_func():

    # get connection to snowflake
    conn = get_snowflake_conn()

    if request.method == 'GET':

        cs = conn.cursor(DictCursor)

        if 'id' in request.args:

            try:
                cs.execute("SELECT * FROM profiles WHERE id = %s", (request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            data = {'data': rows}

            resp = jsonify(data)
            resp.status_code = 200

            return resp

        else:

            try:
                cs.execute("SELECT * FROM profiles")
                rows = cs.fetchall()
            except Exception as e:

                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            data = {'data': rows}

            resp = jsonify(data)
            resp.status_code = 200

            return resp

    elif request.method == 'POST':

        req_data = request.get_json()

        id = get_generated_id()

        cs = conn.cursor(DictCursor)

        try:
            cs.execute(
                "INSERT INTO profiles(id, first_name, last_name, avatar, username, password) "
                "VALUES(%s, %s, %s, %s, %s, %s)", (
                    id,
                    req_data['first_name'],
                    req_data['last_name'],
                    req_data['avatar'],
                    req_data['username'],
                    req_data['password']
                ))
        except Exception as e:

            conn.rollback()
            raise(e)

        finally:
            cs.close()
            close_snowflake_conn(conn)

        # data = {'data': profiles}

        # resp = jsonify(data)
        # resp.status_code = 201

        return 'profile created 201'

    elif request.method == 'PUT':

        if 'id' in request.args:

            cs = conn.cursor(DictCursor)

            req_data = request.get_json()

            try:
                cs.execute("UPDATE profiles SET first_name=%s, last_name=%s, avatar=%s, username=%s, password=%s WHERE id=%s",
                           (req_data['first_name'], req_data['last_name'], req_data['avatar'], req_data['username'], req_data['password'], request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                conn.rollback()
                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            if rows is None:

                id_not_found = 'id {} not found!'.format(request.args['id'])

                data = {'error': id_not_found}

                resp = jsonify(data)
                resp.status_code = 404

                return resp

            data = {'data': rows}

            resp = jsonify(data)
            # resp.status_code = 200

            return resp

        else:

            no_id_found = 'You must pass an id for this action.  No id found!'

            data = {'error': no_id_found}

            resp = jsonify(data)
            resp.status_code = 404

            return resp

    elif request.method == 'DELETE':

        cs = conn.cursor(DictCursor)

        if 'id' in request.args:

            try:
                cs.execute("DELETE FROM profiles WHERE id = %s", (request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                conn.rollback()
                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            if rows is None:

                id_not_found = 'id {} not found!'.format(request.args['id'])

                data = {'error': id_not_found}

                resp = jsonify(data)
                resp.status_code = 404

                return resp

            message = 'Record with id {} was deleted'.format(request.args['id'])
            return message

        else:

            no_id_found = 'You must pass an id for this action.  No id found!'

            data = {'error': no_id_found}

            resp = jsonify(data)
            resp.status_code = 404

            return resp




'''
CRUD for trackable_categories table

The trackable categories are:

 Activities,
 Diet,
 Wellness

'''

@app.route("/category", methods = ['GET', 'POST', 'PUT', 'DELETE'])
def category_func():

    # get connection to snowflake
    conn = get_snowflake_conn()

    if request.method == 'GET':

        cs = conn.cursor(DictCursor)

        if 'id' in request.args:

            try:
                cs.execute("SELECT * FROM trackable_categories WHERE id = %s", (request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            data = {'data': rows}

            resp = jsonify(data)
            resp.status_code = 200

            return resp

        else:

            try:
                cs.execute("SELECT * FROM trackable_categories")
                rows = cs.fetchall()
            except Exception as e:

                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            data = {'data': rows}

            resp = jsonify(data)
            resp.status_code = 200

            return resp

    elif request.method == 'POST':

        req_data = request.get_json()

        id = get_generated_id()

        cs = conn.cursor(DictCursor)

        try:
            cs.execute(
                "INSERT INTO trackable_categories(id, name) "
                "VALUES(%s, %s)", (
                    id,
                    req_data['name']
                ))
        except Exception as e:

            conn.rollback()
            raise(e)

        finally:
            cs.close()
            close_snowflake_conn(conn)

        return 'profile created 201'

    elif request.method == 'PUT':

        if 'id' in request.args:

            cs = conn.cursor(DictCursor)

            req_data = request.get_json()

            try:
                cs.execute("UPDATE trackable_categories SET name=%s WHERE id=%s",
                           (req_data['name'], request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                conn.rollback()
                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            if rows is None:

                id_not_found = 'id {} not found!'.format(request.args['id'])

                data = {'error': id_not_found}

                resp = jsonify(data)
                resp.status_code = 404

                return resp

            data = {'data': rows}

            resp = jsonify(data)
            # resp.status_code = 200

            return resp

        else:

            no_id_found = 'You must pass an id for this action.  No id found!'

            data = {'error': no_id_found}

            resp = jsonify(data)
            resp.status_code = 404

            return resp

    elif request.method == 'DELETE':

        cs = conn.cursor(DictCursor)

        if 'id' in request.args:

            try:
                cs.execute("DELETE FROM trackable_categories WHERE id = %s", (request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                conn.rollback()
                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            if rows is None:

                id_not_found = 'id {} not found!'.format(request.args['id'])

                data = {'error': id_not_found}

                resp = jsonify(data)
                resp.status_code = 404

                return resp

            message = 'Record with id {} was deleted'.format(request.args['id'])
            return message

        else:

            no_id_found = 'You must pass an id for this action.  No id found!'

            data = {'error': no_id_found}

            resp = jsonify(data)
            resp.status_code = 404

            return resp



'''
Get trackables by profile_id

'''

@app.route("/trackable/<profile_id>", methods = ['GET'])
def trackable_by_profile_id_func(profile_id):

    # get connection to snowflake
    conn = get_snowflake_conn()

    cs = conn.cursor(DictCursor)

    if profile_id:

        try:
            cs.execute("SELECT * FROM trackables WHERE profile_id = %s", (profile_id))
            rows = cs.fetchall()
        except Exception as e:

            raise(e)

        finally:
            cs.close()
            close_snowflake_conn(conn)

        data = {'data': rows}

        resp = jsonify(data)
        resp.status_code = 200

        return resp


'''
CRUD for trackables table

'''

@app.route("/trackable", methods = ['GET', 'POST', 'PUT', 'DELETE'])
def trackable_func():

    # get connection to snowflake
    conn = get_snowflake_conn()

    if request.method == 'GET':

        cs = conn.cursor(DictCursor)

        if 'id' in request.args:

            try:
                cs.execute("SELECT * FROM trackables WHERE id = %s", (request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            data = {'data': rows}

            resp = jsonify(data)
            resp.status_code = 200

            return resp

        else:

            try:
                cs.execute("SELECT * FROM trackables")
                rows = cs.fetchall()
            except Exception as e:

                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            data = {'data': rows}

            resp = jsonify(data)
            resp.status_code = 200

            return resp

    elif request.method == 'POST':

        req_data = request.get_json()

        id = get_generated_id()

        cs = conn.cursor(DictCursor)

        try:
            cs.execute(
                "INSERT INTO trackables(id, profile_id, trackable_category_id, name) "
                "VALUES(%s, %s, %s, %s)", (
                    id,
                    req_data['profile_id'],
                    req_data['trackable_category_id'],
                    req_data['name']
                ))
        except Exception as e:

            conn.rollback()
            raise(e)

        finally:
            cs.close()
            close_snowflake_conn(conn)

        return 'profile created 201'

    elif request.method == 'PUT':

        if 'id' in request.args:

            cs = conn.cursor(DictCursor)

            req_data = request.get_json()

            try:
                cs.execute("UPDATE trackables SET profile_id=%s, trackable_category_id=%s, name=%s WHERE id=%s",
                           (req_data['profile_id'], req_data['trackable_category_id'], req_data['name'], request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                conn.rollback()
                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            if rows is None:

                id_not_found = 'id {} not found!'.format(request.args['id'])

                data = {'error': id_not_found}

                resp = jsonify(data)
                resp.status_code = 404

                return resp

            data = {'data': rows}

            resp = jsonify(data)
            # resp.status_code = 200

            return resp

        else:

            no_id_found = 'You must pass an id for this action.  No id found!'

            data = {'error': no_id_found}

            resp = jsonify(data)
            resp.status_code = 404

            return resp

    elif request.method == 'DELETE':

        cs = conn.cursor(DictCursor)

        if 'id' in request.args:

            try:
                cs.execute("DELETE FROM trackables WHERE id = %s", (request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                conn.rollback()
                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            if rows is None:

                id_not_found = 'id {} not found!'.format(request.args['id'])

                data = {'error': id_not_found}

                resp = jsonify(data)
                resp.status_code = 404

                return resp

            message = 'Record with id {} was deleted'.format(request.args['id'])
            return message

        else:

            no_id_found = 'You must pass an id for this action.  No id found!'

            data = {'error': no_id_found}

            resp = jsonify(data)
            resp.status_code = 404

            return resp


'''
Get trackable_log by trackable_id

'''

@app.route("/log/<trackable_id>", methods = ['GET'])
def log_by_trackable_id_func(trackable_id):

    # get connection to snowflake
    conn = get_snowflake_conn()

    cs = conn.cursor(DictCursor)

    if trackable_id:

        try:
            cs.execute("SELECT * FROM trackable_log WHERE trackable_id = %s", (trackable_id))
            rows = cs.fetchall()
        except Exception as e:

            raise(e)

        finally:
            cs.close()
            close_snowflake_conn(conn)

        data = {'data': rows}

        resp = jsonify(data)
        resp.status_code = 200

        return resp


'''
Get trackable_log by profile_id

'''

@app.route("/log/useprofileid/<profile_id>", methods = ['GET'])
def log_by_profile_id_func(profile_id):

    # get connection to snowflake
    conn = get_snowflake_conn()

    cs = conn.cursor(DictCursor)

    if profile_id:

        try:
            cs.execute("select * from trackable_log cross join trackables where profile_id = %s", (profile_id))
            rows = cs.fetchall()
        except Exception as e:

            raise(e)

        finally:
            cs.close()
            close_snowflake_conn(conn)

        data = {'data': rows}

        resp = jsonify(data)
        resp.status_code = 200

        return resp


'''
CRUD for trackable_log table

'''

@app.route("/log", methods = ['GET', 'POST', 'PUT', 'DELETE'])
def log_func():

    # get connection to snowflake
    conn = get_snowflake_conn()

    if request.method == 'GET':

        cs = conn.cursor(DictCursor)

        if 'id' in request.args:

            try:
                cs.execute("SELECT * FROM trackable_log WHERE id = %s", (request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            data = {'data': rows}

            resp = jsonify(data)
            resp.status_code = 200

            return resp

        else:

            try:
                cs.execute("SELECT * FROM trackable_log")
                rows = cs.fetchall()
            except Exception as e:

                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            data = {'data': rows}

            resp = jsonify(data)
            resp.status_code = 200

            return resp

    elif request.method == 'POST':

        req_data = request.get_json()

        id = get_generated_id()

        cs = conn.cursor(DictCursor)

        try:
            cs.execute(
                "INSERT INTO trackable_log(id, trackable_id, log_date, points_earned)"
                "VALUES(%s, %s, %s, %s)", (
                    id,
                    req_data['trackable_id'],
                    req_data['log_date'],
                    int(req_data['points_earned'])
                ))
        except Exception as e:

            conn.rollback()
            raise(e)

        finally:
            cs.close()
            close_snowflake_conn(conn)

        return 'profile created 201'

    elif request.method == 'PUT':

        if 'id' in request.args:

            cs = conn.cursor(DictCursor)

            req_data = request.get_json()

            try:
                cs.execute("UPDATE trackable_log SET trackable_id=%s, log_date=%s, points_earned=%s WHERE id=%s",
                           (req_data['trackable_id'], req_data['log_date'], int(req_data['points_earned']), request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                conn.rollback()
                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            if rows is None:

                id_not_found = 'id {} not found!'.format(request.args['id'])

                data = {'error': id_not_found}

                resp = jsonify(data)
                resp.status_code = 404

                return resp

            data = {'data': rows}

            resp = jsonify(data)
            # resp.status_code = 200

            return resp

        else:

            no_id_found = 'You must pass an id for this action.  No id found!'

            data = {'error': no_id_found}

            resp = jsonify(data)
            resp.status_code = 404

            return resp

    elif request.method == 'DELETE':

        cs = conn.cursor(DictCursor)

        if 'id' in request.args:

            try:
                cs.execute("DELETE FROM trackable_log WHERE id = %s", (request.args['id']))
                rows = cs.fetchone()
            except Exception as e:

                conn.rollback()
                raise(e)

            finally:
                cs.close()
                close_snowflake_conn(conn)

            if rows is None:

                id_not_found = 'id {} not found!'.format(request.args['id'])

                data = {'error': id_not_found}

                resp = jsonify(data)
                resp.status_code = 404

                return resp

            message = 'Record with id {} was deleted'.format(request.args['id'])
            return message

        else:

            no_id_found = 'You must pass an id for this action.  No id found!'

            data = {'error': no_id_found}

            resp = jsonify(data)
            resp.status_code = 404

            return resp
