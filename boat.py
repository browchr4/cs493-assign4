from flask import Blueprint, request, jsonify
from google.cloud import datastore
import json
import constants

client = datastore.Client()

bp = Blueprint('boat', __name__, url_prefix='/boats')

@bp.route('/', methods=['POST','GET'])
def boats_get_post():
    if request.method == 'POST':
        content = request.get_json()
        if 'name' not in content.keys():
            return("need a name", 400)
        if 'type' not in content.keys():
            return("need a type", 400)
        if 'length' not in content.keys():
            return("need a length", 400)
        new_boat = datastore.entity.Entity(key=client.key(constants.boats))
        new_boat.update({'name': content['name'], 'type': content['type'],
          'length': content['length'], 'loads': []})
        client.put(new_boat)
        return str(new_boat.key.id)
    elif request.method == 'GET':
        query = client.query(kind=constants.boats)
        q_limit = int(request.args.get('limit', '3'))
        q_offset = int(request.args.get('offset', '0'))
        l_iterator = query.fetch(limit= q_limit, offset=q_offset)
        pages = l_iterator.pages
        results = list(next(pages))
        if l_iterator.next_page_token:
            next_offset = q_offset + q_limit
            next_url = request.base_url + "?limit=" + str(q_limit) + "&offset=" + str(next_offset)
        else:
            next_url = None
        for e in results:
            selfLink = request.host_url + 'boats/' + str(e.key.id)
            e["self"] = selfLink
        output = {"boats": results}
        if next_url:
            output["next"] = next_url
        return json.dumps(output)
    else:
        return 'Method not recogonized'

@bp.route('/<id>', methods=['DELETE', 'GET'])
def boats_put_delete_get(id):
    if request.method == 'DELETE':
        boat_key = client.key(constants.boats, int(id))
        boat = client.get(key=boat_key)
        if boat is None:
            return('Invalid boat ID', 400)
        else:
            client.delete(boat_key)
            return ('', 200)
    elif request.method == 'GET':
        boat_key = client.key(constants.boats, int(id))
        boat = client.get(key=boat_key)
        if boat is None:
            return('Invalid boat ID', 400)
        else:
            for x in boat['loads']:
                x['self'] = request.host_url + '/loads/' + x['id']
            return (boat, 200)
    else:
        return 'Method not recogonized'

@bp.route('/<lid>/loads/<gid>', methods=['PUT','DELETE'])
def add_delete_reservation(lid, gid):
    if request.method == 'PUT':
        boat_key = client.key(constants.boats, int(lid))
        boat = client.get(key=boat_key)
        if boat is None:
            return('Invalid boat ID', 404)
        load_key = client.key(constants.loads, int(gid))
        load = client.get(key=load_key)
        if load is None:
            return('Invalid load ID', 404)
        if load['boat'] != '':
            return('That load is already assigned', 403)
        else:
            boat['loads'].append(str(gid))
            client.put(boat)
            load['boat'] = str(lid)
            client.put(load)
            return(boat, 200)
    if request.method == 'DELETE':
        boat_key = client.key(constants.boats, int(lid))
        boat = client.get(key=boat_key)
        if boat is None:
            return('Invalid boat ID', 404)
        load_key = client.key(constants.loads, int(gid))
        load = client.get(key=load_key)
        if load is None:
            return('Invalid load ID', 404)
        if gid in boat['loads']:
            boat['loads'].remove(gid)
            client.put(boat)
            load['boat'] = ''
            client.put(load)
            return(boat, 200)
        else:
            return('Invalid ID entered', 404)

@bp.route('/<id>/loads', methods=['GET'])
def get_reservations(id):
    boat_key = client.key(constants.boats, int(id))
    boat = client.get(key=boat_key)
    if boat is None:
        return('Invalid boat ID', 404)
    load_list  = []
    if 'loads' in boat.keys():
        for gid in boat['loads']:
            load_key = client.key(constants.loads, int(gid))
            load = client.get(key=load_key)
            load['self'] = request.host_url + 'loads/' + str(gid)
            load_list.append(load)
        return json.dumps(load_list)
    else:
        return ('Empty', 404)