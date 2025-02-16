import pandas as pd
import glob

# Get all CSV files in the current directory
all_files = glob.glob("../../csv_files/combined_file.csv")

# Create an empty list to hold the dataframes
df_list = []

# Loop through the list of files and read each one into a dataframe
for filename in all_files:
    try:
        df = pd.read_csv(filename)  # Read the CSV file
        df_list.append(df)  # Append the dataframe to the list
    except Exception as e:
        print(f"Error reading {filename}: {e}")

# Concatenate all dataframes in the list into a single dataframe
combined_df = pd.concat(df_list, ignore_index=True)

# Remove duplicate rows (optional)
combined_df = combined_df.drop_duplicates()

# Specify the column for which you want to find distinct values
column_name = 'citywide_utilization_rate'  # Replace with your actual column name

# Method 1: Using unique()
distinct_values = combined_df[column_name].unique()
print("Distinct values using unique():")
print(distinct_values)

# Method 2: Using drop_duplicates()
distinct_values_df = combined_df[column_name].drop_duplicates()
print("\nDistinct values using drop_duplicates():")
print(distinct_values_df.tolist())  # Convert to list for better readability
