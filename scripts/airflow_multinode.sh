#!/usr/bin/env bash

# By B Layer from https://unix.stackexchange.com/a/403894
for param; do 
    [[ ! $param == 'ssh' ]] && newparams+=("$param")
done
set -- "${newparams[@]}"  # filter out 'ssh' from arguments

MASTER_NODE=$1

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# referred to https://github.com/eth-easl/loader/blob/main/scripts/setup/create_multinode.sh

server_exec() {
    ssh -oStrictHostKeyChecking=no -p 22 "$1" "$2";
}

common_init() {
    internal_init() {
        server_exec $1 "git clone --depth=1 https://github.com/vhive-serverless/vhive.git"
        server_exec $1 "mkdir -p /tmp/vhive-logs"
        server_exec $1 "export DEBIAN_FRONTEND="noninteractive" && ./vhive/scripts/cloudlab/setup_node.sh stock-only > >(tee -a /tmp/vhive-logs/setup_node.stdout) 2> >(tee -a /tmp/vhive-logs/setup_node.stderr >&2)"
        server_exec $1 'sudo screen -dmS containerd bash -c "containerd > >(tee -a /tmp/vhive-logs/containerd.stdout) 2> >(tee -a /tmp/vhive-logs/containerd.stderr >&2)"'
    }

    for node in "$@"
    do
        internal_init "$node" &
    done

    wait
}

function setup_master() {
    echo -e "${BLUE}Setting up master node: $MASTER_NODE${NC}"

    server_exec "$MASTER_NODE" 'tmux new -s master -d'

    server_exec "$MASTER_NODE" 'tmux send -t master "cd vhive && ./scripts/cluster/create_multinode_cluster.sh stock-only > >(tee -a /tmp/vhive-logs/create_multinode_cluster.stdout) 2> >(tee -a /tmp/vhive-logs/create_multinode_cluster.stderr >&2)" ENTER'

    # Get the join token from k8s.
    while [ ! "$LOGIN_TOKEN" ]
    do
        sleep 1
        server_exec "$MASTER_NODE" 'tmux capture-pane -t master -b token'
        LOGIN_TOKEN="$(server_exec "$MASTER_NODE" 'tmux show-buffer -b token | grep -B 3 "All nodes need to be joined"')"
        echo -e "${RED}$LOGIN_TOKEN${NC}"
    done
    # cut of last line
    LOGIN_TOKEN=${LOGIN_TOKEN%[$'\t\r\n']*}
    # remove the \
    LOGIN_TOKEN=${LOGIN_TOKEN/\\/}
    # remove all remaining tabs, line ends and returns
    LOGIN_TOKEN=${LOGIN_TOKEN//[$'\t\r\n']}
}

function setup_workers() {
    internal_setup() {
        node=$1

        echo -e "${BLUE}Setting up worker node: $node${NC}"
        server_exec $node "./vhive/scripts/cluster/setup_worker_kubelet.sh stock-only > >(tee -a /tmp/vhive-logs/setup_worker_kubelet.stdout) 2> >(tee -a /tmp/vhive-logs/setup_worker_kubelet.stderr >&2)"

        server_exec $node "sudo ${LOGIN_TOKEN} > >(tee -a /tmp/vhive-logs/kubeadm_join.stdout) 2> >(tee -a /tmp/vhive-logs/kubeadm_join.stderr >&2)"
        echo -e "${BLUE}Worker node $node has joined the cluster.${NC}"
    }

    for node in "$@"
    do
        internal_setup "$node" &
    done

    wait
}

function setup_master_airflow() {
    
    echo -e "${BLUE}Setting up airflow on master node: $MASTER_NODE${NC}"
    server_exec "$MASTER_NODE" "git clone https://github.com/vhive-serverless/airflow.git"
    server_exec "$MASTER_NODE" "export DEBIAN_FRONTEND="noninteractive" && cd airflow && ./scripts/setup_airflow.sh"
    server_exec "$MASTER_NODE" "sudo apt install -y docker.io"
    server_exec "$MASTER_NODE" "sudo chmod 666 /var/run/docker.sock"
    server_exec "$MASTER_NODE" "sudo apt-get install -y htop python3-pip"
    server_exec "$MASTER_NODE" "pip install pendulum"
    server_exec "$MASTER_NODE" "curl -sS https://webi.sh/k9s | sh"
    server_exec "$MASTER_NODE" "source ~/.config/envman/PATH.env"
}

function setup_worker_airflow(){
    make_directory(){
        node=$1
        server_exec "$node" "sudo mkdir -p /mnt/data{0..19} && sudo chmod 777 /mnt/data*"
        echo -e "${RED}Make directory and give permission: $node${NC}"
    }

    for node in "$@"
    do
        make_directory "$node" &
    done

    wait
}
###############################################
######## MAIN SETUP PROCEDURE IS BELOW ########
###############################################

{
    # Set up all nodes including the master
    common_init "$@"

    shift # make argument list only contain worker nodes (drops master node)

    setup_master
    setup_workers "$@"

    # Notify the master that all nodes have joined the cluster
    server_exec $MASTER_NODE 'tmux send -t master "y" ENTER'
    echo -e "${BLUE}Cluster setup finalised.${NC}"

    # Setup airflow on master node and worker nodes
    setup_worker_airflow "$@"
    setup_master_airflow
    echo -e "${BLUE}Master node $MASTER_NODE finalised.${NC}"
}