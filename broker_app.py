"""
Flask app to create a broker
"""
from flask import Flask
from flask import request
from flask import jsonify
import requests
from models import Broker
import yaml
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='config file path', type=str)
parser.add_argument('-n', '--num_brokers', help='Number of brokers initialised', type=int)
args = parser.parse_args()
config=None
with open(args.config) as f:
    config = yaml.safe_load(f)

selfNode=config['SERVER_PORT']+10
partnerNodes=[1010+i*100 for i in range(args.num_brokers) if ((1010+i*100) != selfNode)]
mqs = Broker(config=config,selfNode=selfNode,partnerNodes=partnerNodes)
i=0
while not mqs._isReady():
    i+=1
    time.sleep(5)
    print(f"Contacting other brokers... Time Elapsed: {i*5} seconds")
print("Broker Ready. Leader: "+str(mqs._getLeader()))
app = Flask(__name__)

# Create tables if persistent: Use only for testing. During actual runs. Tables should not
# be dropped and recreated
@app.before_first_request
def create_tables():
    """
    Create tables if persistent
    """
    if config['IS_PERSISTENT']:
        mqs.reset_dbms()

# Routes
@app.route('/')
def index():
    return "DS-Connectify", 200

@app.route('/topics', methods=['POST'])
def createTopic():
    """
    Create a topic
    """
    req = request.json
    if req is None:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    topicName = req['topic_name']
    selfNode=req['selfNode']
    partnerNodes=req['partnerNodes']
    
    try:
        mqs.create_topic(topic_name=topicName,selfNode=selfNode,
                         partnerNodes=partnerNodes)
        resp = {
            "status": "success",
            "message": f'Topic {topicName} created successfully',
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/topics', methods=['GET'])
def listTopic():
    """
    List all topics
    """
    try:
        topics = mqs.list_topics()
        resp = {
            "status": "success",
            "topics": topics,
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/producer/produce', methods=['POST'])
def publish():
    """
    Publish a message to the queue
    """
    req = request.json
    if req is None:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    
    ## Check whether producer ID is valid and it is registered under the topic in the Broker Manager
    topicName = req['topic_name']
    message = req['message']
    try:
        res=mqs.enqueue(topic_name=topicName, message=message)
        if res!=None: ## Then there is an error
            resp = {
                "status": "failure",
                "message": str(res),
            }
            return jsonify(resp), 400
        resp = {
            "status": "success",
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/consumer/consume', methods=['GET'])
def retrieve():
    """
    Retrieve a message from the queue
    """
    req = request.json
    if req is None:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    topicName = req['topic_name']
    offset = req['offset']
    try:
        message = mqs.dequeue(topic_name=topicName, offset=int(offset))
        if type(message)==str: 
            resp = {
                "status": "failure",
                "message": str(message[10:]), ## First 10 letters are "Exception "
            }
            return jsonify(resp), 400
        resp = {
            "status": "success",
            "message": str(message.message),
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

if __name__ == "__main__":
    app.run(debug=False,port=config['SERVER_PORT'])
