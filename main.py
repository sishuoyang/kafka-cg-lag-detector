import argparse
import subprocess
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

def parse_arguments():
    parser = argparse.ArgumentParser(description="Analyze consumer group lag between two Kafka clusters.")
    
    # Cluster 1 arguments
    parser.add_argument("--cluster1-bootstrap-server", required=True, help="Bootstrap server for Kafka cluster 1.")
    parser.add_argument("--cluster1-command-config", required=True, help="Command config file path for Kafka cluster 1.")
    parser.add_argument("--cluster1-cli-dir", required=True, help="Kafka CLI tool directory for Kafka cluster 1.")

    # Cluster 2 arguments
    parser.add_argument("--cluster2-bootstrap-server", required=True, help="Bootstrap server for Kafka cluster 2.")
    parser.add_argument("--cluster2-command-config", required=True, help="Command config file path for Kafka cluster 2.")
    parser.add_argument("--cluster2-cli-dir", required=True, help="Kafka CLI tool directory for Kafka cluster 2.")

    # Summary option
    parser.add_argument("--summary", action="store_true", help="Display a summary of the offset lag.")

    return parser.parse_args()

def run_kafka_consumer_groups(cluster_cli_dir, bootstrap_server, command_config):
    command = [
        f"{cluster_cli_dir}/kafka-consumer-groups",
        "--bootstrap-server", bootstrap_server,
        "--command-config", command_config,
        "--all-groups",
        "--describe"
    ]
    try:
        output = subprocess.check_output(command, universal_newlines=True, stderr=subprocess.DEVNULL)  # Using universal_newlines=True for Python 3.6 compatibility
        return output.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing kafka-consumer-groups: {e}")
        return None

def parse_kafka_consumer_groups_output(output):
    if not output:
        return {}

    group_offsets = defaultdict(lambda: defaultdict(dict))
    for line in output.splitlines():
        if not line.strip() or line.startswith("GROUP"):
            continue

        parts = line.split()
        if len(parts) >= 5:            
            try:
                group, topic, partition = parts[0], parts[1], int(parts[2])
                current_offset = parts[3]
                if current_offset == "-":
                    continue  # Skip this line if current_offset is not a valid number
                current_offset = int(current_offset)  # Convert current_offset to an integer
                group_offsets[group][topic][partition] = current_offset
            except ValueError:
                print(f"Error parsing current offset for line: {line}")  # Print the line if int conversion fails

    return group_offsets

def compare_offsets(cluster1_offsets, cluster2_offsets):
    matching_groups_count = 0
    differing_groups_info = []
    total_lag = 0
    count = 0
    top_lag_partitions = []  # To store top 10 topic partitions with maximum lag

    for group in cluster1_offsets:
        if group in cluster2_offsets:
            for topic in cluster1_offsets[group]:
                if topic in cluster2_offsets[group]:
                    for partition, offset1 in cluster1_offsets[group][topic].items():
                        if partition in cluster2_offsets[group][topic]:
                            offset2 = cluster2_offsets[group][topic][partition]
                            if offset1 == offset2:
                                matching_groups_count += 1
                            else:
                                lag_difference = abs(offset1 - offset2)
                                differing_groups_info.append(
                                    f"Group: {group}, Topic: {topic}, Partition: {partition}, "
                                    f"Cluster 1 Offset: {offset1}, Cluster 2 Offset: {offset2}, "
                                    f"Lag Difference: {lag_difference}"
                                )
                                total_lag += lag_difference
                                count += 1
                                top_lag_partitions.append((group, topic, partition, lag_difference))
                        else:
                            differing_groups_info.append(
                                f"Group: {group}, Topic: {topic}, Partition: {partition} missing in Cluster 2"
                            )
                else:
                    differing_groups_info.append(f"Group: {group}, Topic: {topic} missing in Cluster 2")
        else:
            differing_groups_info.append(f"Group: {group} missing in Cluster 2")

    # Sort and get the top 10 topic partitions with the maximum lag
    top_lag_partitions.sort(key=lambda x: x[3], reverse=True)
    top_lag_partitions = top_lag_partitions[:10]

    return matching_groups_count, differing_groups_info, count, total_lag, top_lag_partitions

def main():
    args = parse_arguments()

    while True:
        print("\nFetching consumer group information from both clusters...")

        # Create a ThreadPoolExecutor to run the commands concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit the tasks to the executor
            futures = [
                executor.submit(run_kafka_consumer_groups, args.cluster1_cli_dir, args.cluster1_bootstrap_server, args.cluster1_command_config),
                executor.submit(run_kafka_consumer_groups, args.cluster2_cli_dir, args.cluster2_bootstrap_server, args.cluster2_command_config)
            ]

            # Retrieve the results as they complete
            results = [future.result() for future in as_completed(futures)]

        # Unpack the results
        cluster1_output, cluster2_output = results

        # Parse the outputs
        cluster1_offsets = parse_kafka_consumer_groups_output(cluster1_output)
        cluster2_offsets = parse_kafka_consumer_groups_output(cluster2_output)

        # Compare the offsets between the two clusters
        matching_groups_count, differing_groups_info, count, total_lag, top_lag_partitions = compare_offsets(cluster1_offsets, cluster2_offsets)

        # Display results
        if args.summary:
            if count > 0:
                average_lag = total_lag / count
                print(f"\n{count} groups have offset differences. The average lag is {average_lag:.2f}.")
                print("\nTop 10 topic partitions with maximum lag:")
                for group, topic, partition, lag_difference in top_lag_partitions:
                    print(f"Group: {group}, Topic: {topic}, Partition: {partition}, Lag Difference: {lag_difference}")
            else:
                print("\nNo groups have offset differences.")
        else:
            if differing_groups_info:
                print("\nGroups with differing offsets between the clusters:")
                for info in differing_groups_info:
                    print(info)
            else:
                print(f"\nAll groups have matching offsets. Total matching groups: {matching_groups_count}")

        # Wait before the next run
        print(f"\nWait for next run...")
        time.sleep(15)

if __name__ == "__main__":
    main()