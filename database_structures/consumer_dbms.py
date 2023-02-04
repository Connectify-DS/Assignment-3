from models import Consumer

class ConsumerDBMS:
    def __init__(self, conn, cur):
        self.conn=conn
        self.cur=cur

    def create_table(self):
        try:
            self.cur.execute("""
            CREATE TABLE IF NOT EXISTS CONSUMERS(
            ID SERIAL PRIMARY KEY NOT NULL,
            TOPIC TEXT NOT NULL,
            OFSET INT NOT NULL);
        """)

            self.conn.commit()
        except:
            self.conn.rollback()

    def register_to_topic(self,topic_name):
        try:
            self.cur.execute("""
            INSERT INTO CONSUMERS (TOPIC,OFSET) 
            VALUES (%s,0)
            RETURNING ID
        """,(topic_name,))

            consumer_id=self.cur.fetchone()
            
            self.conn.commit()
            return consumer_id
        except:
            self.conn.rollback()


    def get_consumer(self,consumer_id):
        try:
            self.cur.execute("""
                SELECT * FROM CONSUMERS
                WHERE ID = %s
            """,(consumer_id,))

            try:
                row=self.cur.fetchone()
                if row is None:
                    raise Exception("Invalid Consumer id")
            except Exception as e:
                raise e

            return Consumer(
                    consumer_id=row[0],
                    topic_name=row[1],
                    cur_topic_queue_offset=row[2]
                )
        except:
            self.conn.rollback()
    
    def increase_offset(self, consumer_id):
        try:
            self.cur.execute("""
                UPDATE CONSUMERS
                SET OFSET = OFSET + 1
                WHERE ID = %s
            """,(consumer_id,))

            self.conn.commit()

            self.cur.execute("""
                SELECT OFSET FROM CONSUMERS
                WHERE ID = %s
            """,(consumer_id,))

            row=self.cur.fetchone()[0]
            return row-1
        except:
            self.conn.rollback()
