import pandas as pd
import pyodbc

# Set up the database connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=stwssbsql01.ad.okstate.edu;DATABASE=brocben;Trusted_Connection=yes;')

# Read CSV files
vehicle_df = pd.read_csv("Z:\Desktop\Vehicles_extract.csv")

# Handle missing values in vehicle_df
vehicle_df.fillna('null', inplace=True)

# Define a function to handle errors during insertion
def handle_insert_error(index, column, value, error):
    print(f"Error occurred while inserting row {index + 1}, column '{column}': {error}")
    print("Problematic value:", value)
    # You can handle the error here, e.g., logging, skipping the row, etc.
    proceed = input("Do you want to proceed? (y/n): ")
    if proceed.lower() != 'y':
        exit()  # Exit the script if the user chooses not to proceed

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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row.values.tolist())  # Pass all values in the row as parameters
        cursor.commit()
    except pyodbc.Error as e:
        # Print out more detailed error information
        print(f"Error occurred while inserting row {index + 1}, column 'Vehicle': {e}")
        print(f"Problematic value: {row.values.tolist()}")
        proceed = input("Do you want to proceed? (y/n): ")
        if proceed.lower() != 'y':
            break  # Exit the loop if the user chooses not to proceed
    finally:
        cursor.close()

# Close the database connection
conn.close()
