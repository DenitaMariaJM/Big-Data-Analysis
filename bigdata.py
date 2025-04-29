import dask.dataframe as dd
import matplotlib.pyplot as plt
import pandas as pd

# Path to your CICIDS 2017 CSV file
csv_file = r"D:\intern\dataset\Wednesday-workingHours.pcap_ISCX.csv"
# Specify the dtypes for the columns causing issues
dtypes = {
    ' Active Std': 'float64',  # Adjusted to float64
    ' Idle Std': 'float64',    # Adjusted to float64
    'Active Mean': 'float64',  # Adjusted to float64
    'Idle Mean': 'float64'    # Adjusted to float64
}

# Load the dataset using Dask and specify dtypes
df = dd.read_csv(csv_file, dtype=dtypes)
# Check the first few rows to understand the data
print(df.head())

label_counts = df[' Label'].value_counts().compute()

# Step 2: Plot the label distribution
plt.figure(figsize=(8, 5))
label_counts.plot(kind='bar', color='skyblue')
plt.title('Distribution of Traffic Types (Labels)')
plt.xlabel('Label')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Step 3: Analyze some features such as 'Flow Duration' and 'Total Fwd Packets'
df['Flow Duration'] = dd.to_numeric(df[' Flow Duration'], errors='coerce')
df['Total Fwd Packets'] = dd.to_numeric(df[' Total Fwd Packets'], errors='coerce')

# Step 4: Visualize the distribution of Flow Duration (Example feature)
flow_duration_mean = df['Flow Duration'].mean().compute()
flow_duration_std = df['Flow Duration'].std().compute()

plt.figure(figsize=(8, 5))
df['Flow Duration'].compute().hist(bins=50, color='skyblue', edgecolor='black')
plt.title(f'Distribution of Flow Duration (Mean: {flow_duration_mean:.2f}, Std: {flow_duration_std:.2f})')
plt.xlabel('Flow Duration')
plt.ylabel('Frequency')
plt.tight_layout()
plt.show()

