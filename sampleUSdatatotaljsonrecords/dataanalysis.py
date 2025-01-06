import json
import pandas as pd
import os

#-----------------------------------------------------------------------------------------------------------------------------------
# Step 1: Define file name and location
file_name = 'Sample Total US.csv'
file_location = os.path.abspath(file_name)

# Step 2: Data Loading
results = pd.read_csv(file_name, encoding='latin1') 

#-----------------------------------------------------------------------------------------------------------------------------------
# Step 3: Data Exploration
total_rows = len(results)
print("Number of rows:", total_rows)
print("Number of columns:", len(results.columns))

#-----------------------------------------------------------------------------------------------------------------------------------
# Step 4: Handle Missing Values
# Example: Fill missing values with empty strings
results.fillna('', inplace=True)

#-----------------------------------------------------------------------------------------------------------------------------------
# Step 5: Convert to JSON
cleaned_data_json = results.to_json(orient='records')

#-----------------------------------------------------------------------------------------------------------------------------------
# Step 6: Save JSON to a text file
with open('cleaned_data.txt', 'w') as f:
    parsed_json = json.loads(cleaned_data_json)
    json.dump(parsed_json, f, indent=4)

print("Cleaned data saved to 'cleaned_data.txt'")

#-----------------------------------------------------------------------------------------------------------------------------------
# Step 7: Count column values and Convert JSON data to DataFrame
cleaned_data_df = pd.DataFrame(parsed_json)

# Count filled values in each column
filled_counts = cleaned_data_df.apply(lambda col: col[col != ''].count())
filled_counts_dict = [{"column_name": col, "count": count} for col, count in filled_counts.items()]  # list comprehension

#-----------------------------------------------------------------------------------------------------------------------------------
# Step 8: Save filled counts to a JSON file along with file information
output_dict = {
    "filename": file_name,
    "filename_location": file_location,
    "total_row_count": total_rows,
    "columns": filled_counts_dict
}

with open('filled_value_counts.json', 'w') as f:
    json.dump(output_dict, f, indent=4)

print("Filled value counts saved to 'filled_value_counts.json'")
