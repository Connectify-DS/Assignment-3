import requests

NUM_TOPICS = 6
NUM_PROUDUCERS_PER_TOPIC = 3
NUM_CONSUMERS_PER_TOPIC = 3
MESSAGE_PER_PRODUCER = 10

load_balancer_url = f"http://127.0.0.1:5000"

print(f"Adding {NUM_TOPICS} topics")
producer_ids = {}
consumer_ids = {}
for num in range(NUM_TOPICS):
    resp = requests.post(
        url = load_balancer_url + "/topics",
        json={"topic_name": f"T{num}"})
    producer_ids[f"T{num}"] = []
    consumer_ids[f"T{num}"] = []
print(25*"-")

print("Registering producers")
for topic_num in range(NUM_TOPICS):
    for _ in range(NUM_PROUDUCERS_PER_TOPIC):
        resp = requests.post(
            url = load_balancer_url + "/producer/register",
            json={"topic_name": f"T{topic_num}"})
        x = resp.json()["message"].split(' ')
        i = 0
        for _ in x:
            if _ == 'ID':
                break
            i += 1
        producer_ids[f"T{topic_num}"].append(int(x[i+1]))
print(25*"-")

print("Producing messages")
for topic_num in range(NUM_TOPICS):
    for producer_id in producer_ids[f"T{topic_num}"]:
        for message_num in range(MESSAGE_PER_PRODUCER):
            resp = requests.post(
                url = load_balancer_url + "/producer/produce",
                json={"producer_id": producer_id, "topic_name": f"T{topic_num}",
                    "message": f"{message_num}th message for Topic T{topic_num} by producer P{producer_id}"})
            assert(resp.status_code == 200)
print(25*"-")

print("Registering consumers")
for topic_num in range(NUM_TOPICS):
    for _ in range(NUM_CONSUMERS_PER_TOPIC):
        resp = requests.post(
            url = load_balancer_url + "/consumer/register",
            json={"topic_name": f"T{topic_num}"})
        x = resp.json()["message"].split(' ')
        i = 0
        for _ in x:
            if _ == 'ID':
                break
            i += 1
        consumer_ids[f"T{topic_num}"].append(int(x[i+1]))
print(25*"-")

print("Consuming messages")
for topic_num in range(NUM_TOPICS):
    for consumer_id in consumer_ids[f"T{topic_num}"]:
        for producer_id in producer_ids[f"T{topic_num}"]:
            for message_num in range(MESSAGE_PER_PRODUCER):
                resp = requests.get(
                    url = load_balancer_url + "/consumer/consume",
                    json={"consumer_id": consumer_id,"topic_name": f"T{topic_num}"})
                assert(resp.status_code == 200)
                with open('consumer_outs/T{}_C{}.txt'.format(topic_num, consumer_id), 'a') as f:
                    f.write(resp.json()["message"] + "\n")
print(25*"-")