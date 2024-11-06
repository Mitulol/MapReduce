#!/bin/bash
#
# mapreduce
#
# Andrew DeOrio <awdeorio@umich.edu>


set -Eeuo pipefail
set -x

LOG_DIR="var/log"

# Commands and arguments
MANAGER_CMD="mapreduce-manager --host localhost --port 6000 --logfile $LOG_DIR/manager.log"
WORKER1_CMD="mapreduce-worker --host localhost --port 6001 --manager-host localhost --manager-port 6000 --logfile $LOG_DIR/worker-6001.log"
WORKER2_CMD="mapreduce-worker --host localhost --port 6002 --manager-host localhost --manager-port 6000 --logfile $LOG_DIR/worker-6002.log"

# Check for already running processes
is_running() {
    pgrep -f "$1" > /dev/null 2>&1
}

# Function to start the mapreduce server
start() {
    if is_running "mapreduce-manager" || is_running "mapreduce-worker"; then
        echo "Error: mapreduce-manager or worker is already running"
        exit 1
    else
        echo "starting mapreduce ..."
        mkdir -p $LOG_DIR
        rm -f $LOG_DIR/manager.log $LOG_DIR/worker-6001.log $LOG_DIR/worker-6002.log
        eval $MANAGER_CMD &
        sleep 2
        eval $WORKER1_CMD &
        eval $WORKER2_CMD &
    fi
}

# Function to stop the mapreduce server
stop() {
    echo "stopping mapreduce ..."
    mapreduce-submit --shutdown --host localhost --port 6000 || true
    sleep 2
    if is_running "mapreduce-manager"; then
        echo "killing mapreduce manager ..."
        pkill -f "mapreduce-manager" || true
    fi
    if is_running "mapreduce-worker"; then
        echo "killing mapreduce worker ..."
        pkill -f "mapreduce-worker" || true
    fi
}

# Function to check the status of the server
status() {
    if is_running "mapreduce-manager"; then
        echo "manager running"
        local manager_running=true
    else
        echo "manager not running"
        local manager_running=false
    fi

    if is_running "mapreduce-worker"; then
        echo "workers running"
        local workers_running=true
    else
        echo "workers not running"
        local workers_running=false
    fi

    $manager_running && $workers_running
}

# Function to restart the server
restart() {
    echo "restarting mapreduce ..."
    stop
    start
}

# Main logic to handle command line arguments
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    restart)
        restart
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart}"
        exit 1
        ;;
esac