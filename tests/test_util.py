import json, requests

def run_request(request_type):

    headers = {
    "accept": "application/json",
    "Content-Type": "application/json"
    }

    data = json.dumps({
        "inputs":{
            "layer": request_type,
            "bbox":"-123.28,49.07,-123.01,49.35",
            "end_time": "2021-12-06T23:00:00Z",
            "start_time": "2021-12-06T00:00:00Z",
        }
    })

    return requests.post("http://localhost:5000/processes/s100/execution", headers=headers, data=data)
