# Kafka Consumer Group Lag Detector

This Python script analyzes the lag between consumer groups across two Kafka clusters. It compares offsets for each topic and partition and provides detailed or summary reports on lag differences. A typical use case is clusters with [Confluent Cluster Linking](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/index.html) and the consumer group offset synchronization is enabled. 

## Note
The script was tested with Confluent Platform 7.1.7 only.

## Features

- **Concurrent Execution**: Fetches consumer group information from both Kafka clusters concurrently for improved performance.
- **Detailed Lag Analysis**: Prints detailed information on consumer groups, topics, and partitions with differing offsets.
- **Summary Report**: Provides a summary report of the number of consumer groups with offset differences and the average lag, along with the top 10 topic partitions with the maximum offset lag.


## Requirements

- Python 3.6 or above
- Kafka CLI tools installed and accessible on the system. This script relies on `kafka-consumer-groups` CLI.

## Installation

1. Clone the repository:
   ```bash
    git clone https://github.com/sishuoyang/kafka-cg-lag-detector.git
    cd kafka-cg-lag-detector
    export stanby_bootstrap="b1.standby.mycluster.com:9093"
    export active_bootstrap='b1.active.mycluster.com:9093'
    export standby_client_properties="/home/ubuntu/standby.properties"
    export active_client_properties='/home/ubuntu/active.properties'
    export clink_bin_path="/opt/confluent/confluent-7.1.7/bin"
    export SCRIPT_PATH=/home/path/to/this/script.py
    python3 $SCRIPT_PATH --cluster1-bootstrap-server $active_bootstrap --cluster1-command-config $active_client_properties --cluster1-cli-dir $clink_bin_path --cluster2-bootstrap-server $stanby_bootstrap --cluster2-command-config $standby_client_properties --cluster2-cli-dir $clink_bin_path --summary
   ```
