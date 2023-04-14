import time
import requests
from threading import Thread
from matplotlib import pyplot as plt

NUM_MESSAGES = 1
MAX_NUM_CONCURRENT_REQUESTS = 60

load_balancer_url = f"http://127.0.0.1:5000"


def add_topic(topic_name):
    _ = requests.post(
        url=load_balancer_url + "/topics",
        json={
            "topic_name": topic_name
        }
    )


def register_producer(topic_name):
    resp = requests.post(
        url=load_balancer_url + "/producer/register",
        json={"topic_name": f"{topic_name}"})


def register_consumer(topic_name):
    resp = requests.post(
        url=load_balancer_url + "/consumer/register",
        json={"topic_name": f"{topic_name}"})


def produce_message(topic_name, pid, message):
    resp = requests.post(
        url=load_balancer_url + "/producer/produce",
        json={"producer_id": pid, "topic_name": f"{topic_name}",
              "message": f"{message}th message for Topic {topic_name} by producer {pid}"})


def consume_message(topic_name, cid):
    resp = requests.get(
        url=load_balancer_url + "/consumer/consume",
        json={"consumer_id": cid, "topic_name": f"{topic_name}"})


def plot_and_save(X, filename):
    plt.clf()
    plt.plot(X)
    plt.savefig(filename)


def test_create_topic():
    curr_id = 0
    throughput = []
    for val in range(1, MAX_NUM_CONCURRENT_REQUESTS+1):
        CUR_TIME = 0
        for _ in range(5):
            threads = []
            for i in range(val):
                topic_name = 'Topic'+str(curr_id)
                curr_id += 1
                thread = Thread(target=add_topic, args=(topic_name,))
                threads.append(thread)
            start = time.time()
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            end = time.time()
            CUR_TIME += end-start
        throughput.append((5*val)/CUR_TIME)

    print("Create Topic Throughput: ", throughput)
    plot_and_save(throughput, 'topic.png')


def test_register_producer():
    curr_id = 0
    throughput = []
    for val in range(1, MAX_NUM_CONCURRENT_REQUESTS+1):
        CUR_TIME = 0
        for _ in range(5):
            threads = []
            for __ in range(val):
                topic_name = 'Topic'+str(curr_id)
                curr_id += 1
                thread = Thread(target=register_producer, args=(topic_name,))
                threads.append(thread)
            start = time.time()
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            end = time.time()
            CUR_TIME += end-start
        throughput.append((5*val)/CUR_TIME)

    print("Register Producer Throughput: ", throughput)
    plot_and_save(throughput, 'reg_prod.png')


def prepro_test_prod_message():
    producer_id_to_topic = {}
    for topic_id in range(5):
        resp = requests.post(
            url=load_balancer_url + "/producer/register",
            json={"topic_name": f"Topic_{topic_id}"})
        x = resp.json()["message"].split(' ')
        i = 0
        for _ in x:
            if _ == 'ID':
                break
            i += 1
        pid = int(x[i+1])
        producer_id_to_topic[pid] = f"Topic_{topic_id}"
    return producer_id_to_topic


def test_produce_message():
    curr_id = 0
    throughput = []
    producer_id_to_topic = prepro_test_prod_message()
    for val in range(1, MAX_NUM_CONCURRENT_REQUESTS+1):
        CUR_TIME = 0
        for pid, topic_name in producer_id_to_topic.items():
            for _ in range(NUM_MESSAGES):
                threads = []
                for __ in range(val):
                    curr_id += 1
                    thread = Thread(target=produce_message, args=(topic_name, pid, str(
                        curr_id)+'th message for Topic {topic_name} by producer P{pid}'))
                    threads.append(thread)
                start = time.time()
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                end = time.time()
                CUR_TIME += end-start
        throughput.append((5*val*NUM_MESSAGES)/CUR_TIME)

    print("Produce Message Throughput: ", throughput)
    plot_and_save(throughput, 'prod_msg.png')


def test_register_consumer():
    curr_id = 0
    throughput = []
    for val in range(1, MAX_NUM_CONCURRENT_REQUESTS+1):
        CUR_TIME = 0
        for _ in range(5):
            threads = []
            for __ in range(val):
                topic_name = 'Topic'+str(curr_id)
                curr_id += 1
                thread = Thread(target=register_consumer, args=(topic_name,))
                threads.append(thread)
            start = time.time()
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            end = time.time()
            CUR_TIME += end-start
        throughput.append((5*val)/CUR_TIME)

    print("Register Consumer Throughput: ", throughput)
    plot_and_save(throughput, 'reg_cnsm.png')


def prepro_test_consume_message():
    consumer_id_to_topic = {}
    for topic_id in range(5):
        resp = requests.post(
            url=load_balancer_url + "/consumer/register",
            json={"topic_name": f"Topic_{topic_id}"})
        x = resp.json()["message"].split(' ')
        i = 0
        for _ in x:
            if _ == 'ID':
                break
            i += 1
        pid = int(x[i+1])
        consumer_id_to_topic[pid] = f"Topic_{topic_id}"
    return consumer_id_to_topic


def test_consume_message():
    curr_id = 0
    throughput = []
    consumer_id_to_topic = prepro_test_consume_message()
    for val in range(1, MAX_NUM_CONCURRENT_REQUESTS+1):
        CUR_TIME = 0
        for cid, topic_name in consumer_id_to_topic.items():
            for _ in range(NUM_MESSAGES):
                threads = []
                for __ in range(val):
                    curr_id += 1
                    thread = Thread(target=consume_message,
                                    args=(topic_name, cid,))
                    threads.append(thread)
                start = time.time()
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                end = time.time()
                CUR_TIME += end-start
        throughput.append((5*val*NUM_MESSAGES)/CUR_TIME)

    print("Consume Message Throughput: ", throughput)
    plot_and_save(throughput, 'read_msg.png')


if __name__ == "__main__":
    # test_create_topic()

    # test_register_consumer()
    # test_register_producer()

    test_produce_message()

    test_consume_message()
