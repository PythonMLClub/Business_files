import numpy as np
from snowflake.snowpark import Session
import pandas as pd
import json
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
# Ordinary Least Squares (OLS) Regression

def connect_to_snowflake(credentials_file):
    # Load credentials from JSON file
    with open(credentials_file, 'r') as f:
        credentials = json.load(f)

    # Create Snowpark session
    session = Session.builder.configs(credentials).create()
    return session

def fetch_data_from_snowflake(session, table_name,limit=10):
    # Execute SQL query to fetch data from the specified table
    table = session.table(table_name)
    limited_table = table.limit(limit)

    df = limited_table.collect()
    
    # Extract records
    records = [row.asDict() for row in df]
    
    # Convert records to Pandas DataFrame
    pandas_df = pd.DataFrame(records)
 
    #---------------updated row-----------------------------#
    # Specify the 'CUSTOMER_NUMBER' for the rows you want to update
    selected_customer_number = 5093443

    selected_index = 5

    # Specify the new values for 'CITY_CODE' and 'REVENUE'
    new_city_code = 123  # Example new city code
    new_revenue = 1000   # Example new revenue

    # # Update rows with the specified 'CUSTOMER_NUMBER'
    # pandas_df.loc[pandas_df['CUSTOMER_NUMBER'] == selected_customer_number, 'CITY_CODE'] = new_city_code
    # pandas_df.loc[pandas_df['CUSTOMER_NUMBER'] == selected_customer_number, 'REVENUE'] = new_revenue

    # Update rows with the specified index
    pandas_df.loc[selected_index, 'CITY_CODE'] = new_city_code
    pandas_df.loc[selected_index, 'REVENUE'] = new_revenue

    # Print the updated DataFrame
    print("updated row:",pandas_df)
    #---------------updated row-----------------------------#

    #------------------------fetch and fliter all column and selected column-----------#
    # Fetch all column names
    column_names = pandas_df.columns.tolist()
    print(column_names)

    # Select specific column names
    selected_column_names = ["CUSTOMER_NUMBER", "CITY_CODE", "REVENUE"]  # Replace with the desired column names
    selected_records = pandas_df.loc[0:5, selected_column_names] 
    print(selected_records)

    # Accessing a specific column value from the first row
    first_column_value = pandas_df.iloc[1]['CITY_CODE']
    print('--------------------------')
    print(first_column_value)
    print('--------------------------')

    # Filter rows based on specific CUSTOMER_NUMBER and CITY_CODE values
    filtered_row = pandas_df[(pandas_df['CUSTOMER_NUMBER'] == 5093443) & (pandas_df['CITY_CODE'] == 239)]
    print(filtered_row)
     #------------------------fetch and fliter all column and selected column-----------#

    csv_file_path = "linear.csv"
    pandas_df.to_csv(csv_file_path, index=False)
    
    return pandas_df


def apply_linear_regression(dataframe, target_col, feature_cols, session):
    # Extract features and target
    X = dataframe[feature_cols]
    y = dataframe[target_col]
    
    # Initialize the Linear Regression model
    lr = LinearRegression()
    
    # Fit the model to the data
    model = lr.fit(X, y)
    
    # Predictions
    y_pred = model.predict(X)
    
    # Create a DataFrame with predictions
    prediction_df = pd.DataFrame({target_col: y, 'Predicted_' + target_col: y_pred})
    
    # Create a DataFrame with predictions using Snowpark
    predictions_data = [(actual, predicted) for actual, predicted in zip(prediction_df[target_col], prediction_df['Predicted_' + target_col])]
    predictions_df = session.createDataFrame(predictions_data, [target_col, 'Predicted_' + target_col])
    
    # Save the predictions as a table in Snowflake
    table_name = "linear_regression_predictions"
    create_table_statement = f"CREATE OR REPLACE TABLE {table_name} ({target_col} FLOAT, Predicted_{target_col} FLOAT)"
    print("Processing..........")
    
    # Execute the create table statement
    session.sql(create_table_statement).collect()
    
    # Insert data into the new table
    for row in predictions_df.collect():
        values = ','.join(map(str, row))
        insert_statement = f"INSERT INTO {table_name} VALUES ({values})"
        # print(insert_statement)

        # Execute the insert statement
        session.sql(insert_statement).collect()

    return model

# Example usage
credentials_file = "snowflake_credentials.json"
snowpark_session = connect_to_snowflake(credentials_file)

# Fetch data from the LINEARREGRESSION table
table_name = "LINEARREGRESSION"
linear_regression_data = fetch_data_from_snowflake(snowpark_session, table_name)

# Define target column name and feature columns
target_column = "REVENUE"
feature_columns = ["STATE", "CITY_CODE"]

# Apply linear regression
linear_regression_model = apply_linear_regression(linear_regression_data, target_column, feature_columns, snowpark_session)







