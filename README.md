# DS-Connectify

Implementation of a distributed queue system.

Authors:\
Mayank Kumar (19CS30029)\
Ishan Goel (19CS30052)\
Shrinivas Khiste (19CS30043)\
Yashica Patodia (19CS10067)\
Shashwat Shukla (19CS10056)

## System Architecture
![](./architecture.png)

## How To Run

Part 1:

- For three nodes with ports 1000, 1100 and 1200, run the following in three different terminals:\
`python atm_raft.py 1000 1100 1200`\
`python atm_raft.py 1100 1000 1200`\
`python atm_raft.py 1200 1100 1000`
- Use the terminals to enter any command for the ATM

Part 2:

- Start the Load Balancer server, Write Manager server and 2 Read Managers\
`bash run.sh`
- Start 4 brokers\
`python broker_app.py -c configs/broker1.yaml -n 4`\
`python broker_app.py -c configs/broker2.yaml -n 4`\
`python broker_app.py -c configs/broker3.yaml -n 4`\
`python broker_app.py -c configs/broker4.yaml -n 4`
- Use Postman to request the Load Balancer at the different end points as per the report. 
