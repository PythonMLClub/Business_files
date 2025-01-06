import numpy as np
from snowflake.snowpark import Session
import pandas as pd
import json
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

def connect_to_snowflake(credentials_file):
    with open(credentials_file, 'r') as f:
        credentials = json.load(f)
    session = Session.builder.configs(credentials).create()
    return session

def fetch_data_from_snowflake(session, table_name, limit=10):
    table = session.table(table_name)
    limited_table = table.limit(limit)  # Limit the number of records fetched
    df = limited_table.collect()
    records = [row.asDict() for row in df]
    pandas_df = pd.DataFrame(records)
    return pandas_df

# Example usage
credentials_file = "snowflake_credentials.json"
snowpark_session = connect_to_snowflake(credentials_file)

# Fetch data from the LINEARREGRESSION table
table_name = "LINEARREGRESSION"
linear_regression_data = fetch_data_from_snowflake(snowpark_session, table_name, limit=100)  # Fetching 100 records

# Choose a threshold for revenue
revenue_threshold = 1000

# Create a binary target variable based on the threshold
linear_regression_data['HighRevenue'] = (linear_regression_data['REVENUE'] > revenue_threshold).astype(int)

# Select features (predictors) and target variable
X = linear_regression_data[['CUSTOMER_NUMBER', 'STATE', 'CITY_CODE', 'ZIP_CODE']]
y = linear_regression_data['HighRevenue']

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize and train logistic regression model
logistic_model = LogisticRegression()
logistic_model.fit(X_train, y_train)

# Predictions on the test set
y_pred = logistic_model.predict(X_test)

# Calculate accuracy
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy:", accuracy)

# # Create a DataFrame for predictions
# predictions_df = pd.DataFrame({
#     'Actual': y_test,          # Actual target values
#     'Predicted': y_pred        # Predicted target values
# })

# print(predictions_df)

# Create a DataFrame for accuracy
accuracy_df = pd.DataFrame({'Accuracy': [accuracy]})

# Convert the accuracy DataFrame to a Snowpark DataFrame
accuracy_df_snowpark = snowpark_session.createDataFrame(accuracy_df)

# Specify the target table name for accuracy
accuracy_table = "ACCURACY_TABLE"

# Define the SQL statement to create the table
create_table_sql = f"CREATE OR REPLACE TABLE {accuracy_table} (Accuracy FLOAT)"
print(create_table_sql)

snowpark_session.sql(create_table_sql).collect()

# Define the SQL statement to insert the accuracy value into the table
insert_sql = f"INSERT INTO {accuracy_table} (Accuracy) VALUES ({accuracy})"
print(insert_sql)

# Execute the SQL statement to insert the accuracy value into the table
snowpark_session.sql(insert_sql).collect()