import pandas as pd
import numpy as np
import pyodbc

# Set up the database connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=stwssbsql01.ad.okstate.edu;DATABASE=TTeam1;Trusted_Connection=yes;')

# Read CSV files
vehicle_df = pd.read_csv(r"C:\Users\brocben\Desktop\OneDrive_1_4-25-2024\Vehicles_extract.csv")
accidents_df = pd.read_csv(r"C:\Users\brocben\Desktop\OneDrive_1_4-25-2024\Accidents_extract.csv")

# Print column names and the number of rows for the vehicle data
print("Vehicle Data Columns:")
print(vehicle_df.columns)
print("Number of rows in Vehicle Data:", len(vehicle_df))

# Print column names and the number of rows for the accident data
print("\nAccident Data Columns:")
print(accidents_df.columns)
print("Number of rows in Accident Data:", len(accidents_df))

# Replace blank spaces with NaN for both dataframes
vehicle_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
accidents_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)

print(vehicle_df.head(10))
print(accidents_df.head(10))

# Convert Accident_Index to string if they are not, to ensure proper comparison
vehicle_df['Accident_Index'] = vehicle_df['Accident_Index'].astype(str)
accidents_df['Accident_Index'] = accidents_df['Accident_Index'].astype(str)

# Check if all Accident_Index in vehicle_df are in accidents_df
in_accident_df = vehicle_df['Accident_Index'].isin(accidents_df['Accident_Index']).all()
# Check if all Accident_Index in accidents_df are in vehicle_df
in_vehicle_df = accidents_df['Accident_Index'].isin(vehicle_df['Accident_Index']).all()

print("All Vehicle Accident_Index in Accident DataFrame:", in_accident_df)
print("All Accident Accident_Index in Vehicle DataFrame:", in_vehicle_df)
