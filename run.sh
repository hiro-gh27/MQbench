#!/bin/sh
host="imac"
host3="imac3"
host17="akimac17"
runPath="/Users/hiroki/PIQT-oct/run.sh"
runPath17="/Users/hiroki17/PIQT-oct/run.sh"


for num in 8.0 8.4 8.8 9.2 9.6 10.0 10.4 10.8 11.2 11.6 12.0 12.4 12.8 13.2 13.6 14.0 14.4 14.8 15.2
do
    count=0
    while [ $count -lt 10 ]
    do

    ssh $host bash $runPath &
    sleep 5
    ssh $host17 bash $runPath17 &
    sleep 20
    pid=$(ssh $host /usr/local/bin/pidof java)
    pid17=$(ssh $host17 /usr/local/bin/pidof java)
    ./case1 -load=$num -broker=PIQT_12_01_case1
    ssh $host kill $pid
    ssh $host17 kill $pid17

    ssh $host bash $runPath &
    sleep 5
    ssh $host3 bash $runPath &
    sleep 5
    ssh $host17 bash $runPath17 &
    sleep 20
    pid=$(ssh $host /usr/local/bin/pidof java)
    pid3=$(ssh $host3 /usr/local/bin/pidof java)
    pid17=$(ssh $host17 /usr/local/bin/pidof java)
    ./case2 -load=$num -broker=PIQT_12_01_case2
    ssh $host kill $pid
    ssh $host3 kill $pid3
    ssh $host17 kill $pid17

    ssh $host bash $runPath &
    sleep 5
    ssh $host3 bash $runPath &
    sleep 5
    ssh $host17 bash $runPath17 &
    sleep 20
    pid=$(ssh $host /usr/local/bin/pidof java)
    pid3=$(ssh $host3 /usr/local/bin/pidof java)
    pid17=$(ssh $host17 /usr/local/bin/pidof java)
    ./case3 -load=$num -broker=PIQT_12_01_case3
    ssh $host kill $pid
    ssh $host3 kill $pid3
    ssh $host17 kill $pid17

	let count++
    done
done
