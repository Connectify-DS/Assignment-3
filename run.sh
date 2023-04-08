#!/bin/bash

function kill_all {
    pkill -9 python
}

trap "kill_all" SIGINT SIGTERM

python load_balancer_app.py &
sleep 5

python write_manager_app.py &
sleep 5

python read_manager_app.py -c configs/rm1.yaml &
python read_manager_app.py -c configs/rm2.yaml &
sleep 5

python broker_app.py -c configs/broker1.yaml &
python broker_app.py -c configs/broker2.yaml &
python broker_app.py -c configs/broker3.yaml &
python broker_app.py -c configs/broker4.yaml &
