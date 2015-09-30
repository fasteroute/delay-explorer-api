from flask import Flask

import requests

from getstuff import Querier

app = Flask(__name__)

q = Querier()
q.setup()

@app.route("/TrainExplorer/<origin>/<destination>/<_type>/<_time>")
def train_explorer_all(origin, destination, _type, _time):
    try:
        if origin == "_":
            origin = None
        if destination == "_":
            destination = None
        if origin is None and destination is None:
            return "{}"
        return(q.get_some_data(origin=origin, destination=destination, _type=_type, _time=_time))
    except Exception as e:
        print(e)
        q.connection.rollback()

@app.route("/CallingPoints/<origin>/<destination>/<uid>")
def calling_points(origin, destination, uid):
    try:
        if origin == "_" or origin == "null":
            origin = None
        if destination == "_" or destination == "null":
            destination = None
        return (q.get_calling_points(origin=origin, destination=destination, uid=uid))
    except Exception as e:
        q.connection.rollback()

@app.route("/Locations/<search>")
def locations(search):
    # TODO implement me.
    return {}

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
