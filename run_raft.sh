#!/bin/bash

function kill_all {
    pkill -9 python
}

trap "kill_all" SIGINT SIGTERM

python atm_raft.py 1010 1020 1030 1040 &
sleep 2
python atm_raft.py 1020 1010 1030 1040 &
sleep 2
python atm_raft.py 1030 1020 1010 1040 &
sleep 2
python atm_raft.py 1040 1020 1030 1010 &
sleep 2
