import pandas as pd
import codecs

# Set display options for easier debugging
pd.options.display.max_columns = None
pd.options.display.max_rows = 40

# Define file paths with raw strings
filename1 = r'C:\Users\brocben\Desktop\archive\Accident_Information.csv'
filename2 = r'C:\Users\brocben\Desktop\archive\Vehicle_Information.csv'

# Read CSV files using ISO-8859-1 encoding
with codecs.open(filename1, 'r', encoding='ISO-8859-1') as f:
    dfAcc = pd.read_csv(f, low_memory=False)

with codecs.open(filename2, 'r', encoding='ISO-8859-1') as f:
    dfVeh = pd.read_csv(f, low_memory=False)

# Sample from the Accident Information DataFrame
dfAcc_sampled = dfAcc.sample(n=300000, random_state=42)  # Use a random state for reproducibility

# Ensure that missing data are explicitly handled as NaN
dfAcc_sampled.fillna(pd.NA, inplace=True)
dfVeh.fillna(pd.NA, inplace=True)

# Filter the Vehicle DataFrame to only include matched Accident_Index from the sampled Accident DataFrame
dfVeh_matched = dfVeh[dfVeh['Accident_Index'].isin(dfAcc_sampled['Accident_Index'])]

# Refilter the Accident DataFrame to ensure all are matched in Vehicle DataFrame
dfAcc_matched = dfAcc_sampled[dfAcc_sampled['Accident_Index'].isin(dfVeh_matched['Accident_Index'])]

# Sort both dataframes by 'Accident_Index'
sorted_dfAcc = dfAcc_matched.sort_values(by='Accident_Index')
sorted_dfVeh = dfVeh_matched.sort_values(by='Accident_Index')

# Save sorted dataframes to CSV files
sorted_dfAcc.to_csv('ProjectDataFiles/Accidents_extract.csv', index=False)
sorted_dfVeh.to_csv('ProjectDataFiles/Vehicles_extract.csv', index=False)

# Print unique counts of 'Accident_Index' to verify integrity
print(f"Unique Accident_Index in Accidents: {sorted_dfAcc['Accident_Index'].nunique()}")
print(f"Unique Accident_Index in Vehicles: {sorted_dfVeh['Accident_Index'].nunique()}")

# Optionally print column names
print(list(dfAcc.columns))
