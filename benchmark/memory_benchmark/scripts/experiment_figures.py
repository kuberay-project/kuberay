import matplotlib.pyplot as plt

# [Experiment 1]:
# Launch a RayCluster with 1 head and no workers. A new cluster is initiated every 20 seconds until
# there are a total of 150 RayCluster custom resources.
num_pods_diff20 = [0, 20, 40, 60, 80, 100, 120, 140, 150]
experiment1 = [
    20.71875,
    23.2421875,
    26.6015625,
    29.453125,
    31.25390625,
    35.21484375,
    34.52734375,
    35.73046875,
    36.19921875,
]

# [Experiment 2]
# In the Kubernetes cluster, there is only 1 RayCluster. Add 5 new worker Pods to this
# RayCluster every 60 seconds until the total reaches 150 Pods.
num_pods_diff10 = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150]
experiment2 = [
    24.49609375,
    24.6875,
    25.9609375,
    28.59375,
    27.8984375,
    29.15625,
    29.7734375,
    32.015625,
    32.74609375,
    33.3203125,
    34.140625,
    34.8515625,
    36.75,
    37.28125,
    38.34375,
    40.4453125,
]
# [Experiment 3]
# Create a 5-node (1 head + 4 workers) RayCluster every 60 seconds until there are 30 RayCluster custom resources.
experiment3 = [
    19.7578125,
    20.8515625,
    22.99609375,
    23.19921875,
    26.0234375,
    25.8984375,
    26.1640625,
    29.43359375,
    29.0859375,
    33.3359375,
    32.89453125,
    34.78125,
    37.890625,
    39.125,
    39.078125,
    41.6328125,
]

# Plotting
plt.figure(figsize=(12, 7))
plt.plot(num_pods_diff20, experiment1, label="Exp 1", marker="o")
plt.plot(num_pods_diff10, experiment2, label="Exp 2", marker="o")
plt.plot(num_pods_diff10, experiment3, label="Exp 3", marker="o")
plt.xlabel("Number of Pods")
plt.ylabel("Memory (MB)")
plt.title("Memory usage vs. Number of Pods")
plt.ylim(0, max(max(experiment1), max(experiment2), max(experiment3)) + 5)
plt.legend()
plt.grid(True, which="both", linestyle="--", linewidth=0.5)
plt.tight_layout()
plt.savefig("benchmark_result.png")
plt.show()
