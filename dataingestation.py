import pandas as pd
import pyodbc

# Set up the database connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=stwssbsql01.ad.okstate.edu;DATABASE=TTeam1;Trusted_Connection=yes;')

# Read CSV files
vehicle_df = pd.read_csv("C:\\Users\\brocben\Desktop\OneDrive_1_4-25-2024\Vehicles_extract.csv")
accidents_df = pd.read_csv("C:\\Users\\brocben\Desktop\OneDrive_1_4-25-2024\Accidents_extract.csv")

# Handle missing values in vehicle_df
vehicle_df.fillna('null', inplace=True)

# # Handle missing values in accidents_df
# accidents_df.fillna('null', inplace=True)

# Print data types of each column
print("Data types of each column:")
print(vehicle_df.dtypes)
# print(accidents_df.dtypes)

# Calculate number of rows and columns
num_rows_vehicle = len(vehicle_df)
num_cols_vehicle = len(vehicle_df.columns)
# num_rows_accidents = len(accidents_df)
# num_cols_accidents = len(accidents_df.columns)

print("\nNumber of rows to be inserted into Vehicle table:", num_rows_vehicle)
print("Number of columns to be inserted into Vehicle table:", num_cols_vehicle)
# print("\nNumber of rows to be inserted into Accident table:", num_rows_accidents)
# print("Number of columns to be inserted into Accident table:", num_cols_accidents)
# print()

# Set the display options to show all columns without truncation
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
# print the head of the data out
print(vehicle_df.head(10))
# print(accidents_df.head(10))

# Define a function to handle errors during insertion
def handle_insert_error(index, column, value, error):
    print(f"Error occurred while inserting row {index + 1}, column '{column}': {error}")
    print("Problematic value:", value)
    # You can handle the error here, e.g., logging, skipping the row, etc.
    proceed = input("Do you want to proceed? (y/n): ")
    if proceed.lower() != 'y':
        exit()  # Exit the script if user chooses not to proceed

# Insert data into Accident table
for index, row in accidents_df.iterrows():
    cursor = conn.cursor()
    try:
        # Attempt to execute the insert query
        cursor.execute("""
            INSERT INTO Accident (Accident_Index, Road_Class_1, Road_Number_1, Road_Class_2, Road_Number_2, Accident_Severity,
                                  Carriageway_Hazards, Date, Day_of_Week, Did_Police_Attend_Scene_of_Accident, Junction_Control,
                                  Junction_Detail, Latitude, Light_Conditions, Local_Authority_District, Local_Authority_Highway,
                                  Location_Easting_OSGR, Location_Northing_OSGR, Longitude, LSOA_of_Accident_Location,
                                  Number_of_Casualties, Number_of_Vehicles, Pedestrian_Crossing_Human_Control,
                                  Pedestrian_Crossing_Physical_Facilities, Police_Force, Road_Surface_Conditions, Road_Type,
                                  Special_Conditions_at_Site, Speed_limit, Time, Urban_or_Rural_Area, Weather_Conditions, Year,
                                  InScotland)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, tuple(row.astype(str).values))  # Convert all values to string to ensure correct formatting
        conn.commit()
    except pyodbc.IntegrityError as e:
        print(f"Skipped duplicate row {index + 1}: {e}")
        continue
    except pyodbc.Error as e:
        handle_insert_error(index, 'unknown_column', row.values.tolist(), e)
        continue
    finally:
        cursor.close()

# Function to handle errors during insertion
def handle_insert_error(index, column, value, error):
    print(f"Error occurred while inserting row {index + 1}, column '{column}': {error}")
    print("Problematic value:", value)
    proceed = input("Do you want to proceed? (y/n): ")
    if proceed.lower() != 'y':
        exit()  # Exit the script if user chooses not to proceed

# Note: Ensure that you are using the correct datatypes and handling NaN or None values correctly before attempting to insert into the database.

# Insert data into Vehicle table
for index, row in vehicle_df.iterrows():
    cursor = conn.cursor()
    try:
        # Attempt to execute the insert query
        cursor.execute("""
            INSERT INTO Vehicle (Accident_Index, Age_Band_of_Driver, Age_of_Vehicle, Driver_Home_Area_Type, Driver_IMD_Decile,
                                 Engine_Capacity_CC, Hit_Object_in_Carriageway, Hit_Object_off_Carriageway, Journey_Purpose_of_Driver,
                                 Junction_Location, Make, Model, Propulsion_Code, Sex_of_Driver, Skidding_and_Overturning,
                                 Towing_and_Articulation, Vehicle_Leaving_Carriageway, Vehicle_Location_Restricted_Lane,
                                 Vehicle_Manoeuvre, Vehicle_Type, Was_Vehicle_Left_Hand_Drive, X1st_Point_of_Impact, Year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?)
        """, row.values[1:])  # Exclude the first element (Vehicle_ID)
        cursor.commit()
    except pyodbc.Error as e:
        # Print out more detailed error information
        print(f"Error occurred while inserting row {index + 1}, column 'Vehicle': {e}")
        print(f"Problematic value: {row.values.tolist()}")
        print("Do you want to proceed? (y/n): ")
        response = input()
        if response.lower() != 'y':
            break  # Exit the loop if the user chooses not to proceed
    finally:
        cursor.close()







# Close the database connection
conn.close()
