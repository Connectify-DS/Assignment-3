from pysyncobj import SyncObj, replicated_sync, replicated
import time

class Partition(SyncObj):
    def __init__(self, topic_table, message_table, topic_name, selfNode, partnerNodes) -> None:
        super(Partition, self).__init__(f"127.0.0.0:{selfNode}", [ f"127.0.0.0:{x}" for x in partnerNodes])
        self.topic_table=topic_table
        self.topic_name=topic_name
        self.message_table=message_table
        print(selfNode,partnerNodes)
        print(self._getLeader())

    @replicated_sync
    def enqueue(self, message: str):
        print("In Partition")
        try:
            topic_queue = self.topic_table.get_topic_queue(self.topic_name)
            if topic_queue is None:
                raise Exception(f"Topic name {self.topic_name} not found")
            message_id = self.message_table.add_message(message)
            topic_queue.enqueue(message_id)
        except Exception as e:
            raise e
    
    @replicated_sync
    def dequeue(self, offset: int):
        print("In partition")
        try:
            topic_queue = self.topic_table.get_topic_queue(self.topic_name)
            if topic_queue is None:
                raise Exception(f"Topic name {self.topic_name} not found")

            size_rem = topic_queue.size() - offset
            if size_rem <= 0:
                raise Exception("No messages left to retrieve")

            if self.persistent:
                message_id = topic_queue.get_at_offset(offset+1)
            else:
                message_id = topic_queue.get_at_offset(offset)
            if message_id:
                message_data = self.message_table.get_message(message_id)
                return message_data
            else:
                raise Exception("Could not retrieve message")
        except Exception as e:
            raise e