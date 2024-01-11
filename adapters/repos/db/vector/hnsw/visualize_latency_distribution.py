import json
import matplotlib.pyplot as plt
import numpy as np

# Load the JSON file
file_path = './LatenciesPerFilter-intervention.json'

with open(file_path, 'r') as file:
    data = json.load(file)

# Displaying the structure of the loaded data
data.keys(), {k: list(data[k].keys()) for k in data.keys()}

# Extracting the latencies for each filter
latencies_filter_0 = data['0']['0']
latencies_filter_1 = data['0']['1']

# Basic statistics
mean_0 = np.mean(latencies_filter_0)
std_dev_0 = np.std(latencies_filter_0)
mean_1 = np.mean(latencies_filter_1)
std_dev_1 = np.std(latencies_filter_1)

# Visualization
plt.figure(figsize=(14, 6))

# Histogram for filter 0
plt.subplot(1, 2, 1)
plt.hist(latencies_filter_0, bins=20, color='blue', alpha=0.7)
plt.title('Latency Distribution for Filter 0')
plt.xlabel('Latency (ms)')
plt.ylabel('Frequency')
plt.axvline(mean_0, color='red', linestyle='dashed', linewidth=1)
plt.text(mean_0 + std_dev_0, max(plt.gca().get_ylim()) / 2, f'Mean: {mean_0:.2f}\nStd Dev: {std_dev_0:.2f}', color='red')

# Histogram for filter 1
plt.subplot(1, 2, 2)
plt.hist(latencies_filter_1, bins=20, color='green', alpha=0.7)
plt.title('Latency Distribution for Filter 1')
plt.xlabel('Latency (ms)')
plt.ylabel('Frequency')
plt.axvline(mean_1, color='red', linestyle='dashed', linewidth=1)
plt.text(mean_1 + std_dev_1, max(plt.gca().get_ylim()) / 2, f'Mean: {mean_1:.2f}\nStd Dev: {std_dev_1:.2f}', color='red')

plt.tight_layout()
plt.show()

print(mean_0, std_dev_0, mean_1, std_dev_1)
