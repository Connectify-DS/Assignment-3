import time
import requests

NUM_TOPICS = 6
NUM_PROUDUCERS_PER_TOPIC = 3
NUM_CONSUMERS_PER_TOPIC = 3
MESSAGE_PER_PRODUCER = 10

load_balancer_url = f"http://127.0.0.1:5000"

TOTAL_TIME = 0
TOTAL_REQUESTS = 0

CUR_TIME = 0
CUR_REQUESTS = 0

print(f"Adding {NUM_TOPICS} topics")
producer_ids = {}
consumer_ids = {}
for num in range(NUM_TOPICS):
    start = time.time()
    resp = requests.post(
        url=load_balancer_url + "/topics",
        json={"topic_name": f"T{num}"})
    end = time.time()
    TOTAL_TIME += (end-start)
    TOTAL_REQUESTS += 1
    CUR_TIME += (end-start)
    CUR_REQUESTS += 1
    producer_ids[f"T{num}"] = []
    consumer_ids[f"T{num}"] = []
print(
    f"Average time taken for a request for adding topic: {CUR_TIME/CUR_REQUESTS} seconds")
print(
    f"Total time taken for {CUR_REQUESTS} requests for adding topic: {CUR_TIME} seconds")
print(25*"-")

CUR_TIME = 0
CUR_REQUESTS = 0

print("Registering producers")
for topic_num in range(NUM_TOPICS):
    for _ in range(NUM_PROUDUCERS_PER_TOPIC):
        start = time.time()
        resp = requests.post(
            url=load_balancer_url + "/producer/register",
            json={"topic_name": f"T{topic_num}"})
        end = time.time()
        TOTAL_TIME += (end-start)
        TOTAL_REQUESTS += 1
        CUR_TIME += (end-start)
        CUR_REQUESTS += 1
        x = resp.json()["message"].split(' ')
        i = 0
        for _ in x:
            if _ == 'ID':
                break
            i += 1
        producer_ids[f"T{topic_num}"].append(int(x[i+1]))
print(
    f"Average time taken for a request for adding producer: {CUR_TIME/CUR_REQUESTS} seconds")
print(
    f"Total time taken for {CUR_REQUESTS} requests for adding producer: {CUR_TIME} seconds")
print(25*"-")

CUR_TIME = 0
CUR_REQUESTS = 0

print("Producing messages")
for topic_num in range(NUM_TOPICS):
    for producer_id in producer_ids[f"T{topic_num}"]:
        for message_num in range(MESSAGE_PER_PRODUCER):
            start = time.time()
            resp = requests.post(
                url=load_balancer_url + "/producer/produce",
                json={"producer_id": producer_id, "topic_name": f"T{topic_num}",
                      "message": f"{message_num}th message for Topic T{topic_num} by producer P{producer_id}"})
            end = time.time()
            TOTAL_TIME += (end-start)
            TOTAL_REQUESTS += 1
            CUR_TIME += (end-start)
            CUR_REQUESTS += 1
            assert (resp.status_code == 200)
print(
    f"Average time taken for a request for producing message: {CUR_TIME/CUR_REQUESTS} seconds")
print(
    f"Total time taken for {CUR_REQUESTS} requests for producing message: {CUR_TIME} seconds")
print(25*"-")

CUR_TIME = 0
CUR_REQUESTS = 0

print("Registering consumers")
for topic_num in range(NUM_TOPICS):
    for _ in range(NUM_CONSUMERS_PER_TOPIC):
        start = time.time()
        resp = requests.post(
            url=load_balancer_url + "/consumer/register",
            json={"topic_name": f"T{topic_num}"})
        end = time.time()
        TOTAL_TIME += (end-start)
        TOTAL_REQUESTS += 1
        CUR_TIME += (end-start)
        CUR_REQUESTS += 1
        x = resp.json()["message"].split(' ')
        i = 0
        for _ in x:
            if _ == 'ID':
                break
            i += 1
        consumer_ids[f"T{topic_num}"].append(int(x[i+1]))
print(
    f"Average time taken for a request for adding consumer: {CUR_TIME/CUR_REQUESTS} seconds")
print(
    f"Total time taken for {CUR_REQUESTS} requests for adding consumer: {CUR_TIME} seconds")
print(25*"-")

CUR_TIME = 0
CUR_REQUESTS = 0

print("Consuming messages")
for topic_num in range(NUM_TOPICS):
    for consumer_id in consumer_ids[f"T{topic_num}"]:
        for producer_id in producer_ids[f"T{topic_num}"]:
            for message_num in range(MESSAGE_PER_PRODUCER):
                start = time.time()
                resp = requests.get(
                    url=load_balancer_url + "/consumer/consume",
                    json={"consumer_id": consumer_id, "topic_name": f"T{topic_num}"})
                end = time.time()
                TOTAL_TIME += (end-start)
                TOTAL_REQUESTS += 1
                CUR_TIME += (end-start)
                CUR_REQUESTS += 1
                # assert(resp.status_code == 200)
                with open('consumer_outs/T{}_C{}.txt'.format(topic_num, consumer_id), 'a') as f:
                    f.write(resp.json()["message"] + "\n")
print(
    f"Average time taken for a request for consuming message: {CUR_TIME/CUR_REQUESTS} seconds")
print(
    f"Total time taken for {CUR_REQUESTS} requests for consuming message: {CUR_TIME} seconds")
print(25*"-")

print(f"Average time taken for a request: {TOTAL_TIME/TOTAL_REQUESTS} seconds")
print(f"Total time taken for {TOTAL_REQUESTS} requests: {TOTAL_TIME} seconds")
