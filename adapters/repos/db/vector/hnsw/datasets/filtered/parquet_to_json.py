import pyarrow.parquet as pq
import pandas as pd
import glob
import json
import os

# Directory containing Parquet files
directory_path = './DBPedia-OpenAI-1M-1536/'

# Output directory for JSON files
output_directory = './DBPedia-OpenAI-1M-1536-JSON/'

# Ensure the output directory exists
os.makedirs(output_directory, exist_ok=True)

# Find all Parquet files in the directory
parquet_files = glob.glob(f'{directory_path}*.parquet')

# Loop through each Parquet file
for parquet_file_path in parquet_files:
    # Open the Parquet file
    parquet_file = pq.ParquetFile(parquet_file_path)

    # Read the Parquet file into a Pandas DataFrame
    df = parquet_file.read().to_pandas()

    # Convert DataFrame to JSON
    json_data = df.to_json(orient='records')

    # Extract the base name of the Parquet file
    base_name = os.path.basename(parquet_file_path)

    # Replace .parquet extension with .json for the output file name
    output_file_name = base_name.replace('.parquet', '.json')

    # Combine the output directory with the new file name
    output_file_path = os.path.join(output_directory, output_file_name)

    # Output JSON data to the file in the new directory
    with open(output_file_path, 'w') as f:
        f.write(json_data)
    
    print(f"Conversion completed. JSON data saved to {output_file_path}")
