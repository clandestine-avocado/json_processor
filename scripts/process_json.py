import os
import json
import pandas as pd
import numpy as np

def iterate_json_files(directory):
    """
    Iterate over all JSON files in the provided directory.
    
    Args:
    directory (str): Path to the directory containing JSON files.
    
    Yields:
    tuple: A tuple containing (file_name, file_path) for each JSON file.
    """
    # Loop through all files in the specified directory
    for file_name in os.listdir(directory):
        # Check if the file has a .json extension
        if file_name.endswith('.json'):
            # Yield the file name and its full path
            yield file_name, os.path.join(directory, file_name)

def process_json_files(directory):
    """
    Process JSON files in the given directory and create dataframes.
    
    Args:
    directory (str): Path to the directory containing JSON files.
    
    Returns:
    dict: A dictionary of dataframes, with keys being the table names.
    """
    # Initialize a dictionary to hold DataFrames for each table
    dataframes = {
        'tblIndividual': pd.DataFrame(),
        'tblRemarks': pd.DataFrame(),
        'tblAssociatedIndividual': pd.DataFrame(),
        'tblAssociatedCountry': pd.DataFrame(),
        'tblAssociatedDocument': pd.DataFrame(),
        'tblAssociatedOrganization': pd.DataFrame()
    }
    
    # Iterate through each JSON file in the directory
    for file_name, file_path in iterate_json_files(directory):
        # Extract the foreign key ID from the file name (without extension)
        fk_id = os.path.splitext(file_name)[0]
        
        # Open and load the JSON data from the file
        with open(file_path, 'r') as file:
            data = json.load(file)
        
        # Ensure data is a list; if it's a single object, convert it to a list
        if not isinstance(data, list):
            data = [data]
        
        # Process each item in the loaded JSON data
        for item in data:
            # Extract single value fields and handle None values
            single_value_data = {k: [v if v is not None else np.nan] for k, v in item.items() if not isinstance(v, (list, dict))}
            # Add the foreign key ID to the single value data
            single_value_data['fkID'] = [fk_id]
            # Create a DataFrame from the single value data
            df_individual = pd.DataFrame(single_value_data)
            # Concatenate the new DataFrame with the existing one for tblIndividual
            dataframes['tblIndividual'] = pd.concat([dataframes['tblIndividual'], df_individual], ignore_index=True)
            
            # Process remarks if they exist and are in list format
            if 'remarks' in item and isinstance(item['remarks'], list):
                df_remarks = pd.DataFrame({'remark': item['remarks'], 'fkID': [fk_id] * len(item['remarks'])})
                # Concatenate the remarks DataFrame into tblRemarks
                dataframes['tblRemarks'] = pd.concat([dataframes['tblRemarks'], df_remarks], ignore_index=True)
            
            # Process associations if they exist and are in dictionary format
            if 'associations' in item and isinstance(item['associations'], dict):
                # Loop through each type of association
                for assoc_type, assoc_list in item['associations'].items():
                    # Check if the association list is indeed a list
                    if isinstance(assoc_list, list):
                        # Create the name for the corresponding DataFrame
                        df_name = f'tblAssociated{assoc_type.capitalize()}'
                        # Create a DataFrame from the association data
                        df = pd.DataFrame({f'{assoc_type}_id': assoc_list, 'fkID': [fk_id] * len(assoc_list)})
                        # Concatenate the association DataFrame if its name is in the dataframes dictionary
                        if df_name in dataframes:
                            dataframes[df_name] = pd.concat([dataframes[df_name], df], ignore_index=True)
    
    # Return the dictionary containing all processed DataFrames
    return dataframes


# Example usage:
if __name__ == "__main__":
    # Define the directory containing the JSON files
    directory = r"C:\Users\kroy2\Documents\python\projects\json_processor\data"
    
    # Process the JSON files and get the resulting DataFrames
    result_dataframes = process_json_files(directory)

    # Ask the user for their choice: print DataFrames or create CSV files
    choice = input("Would you like to print the dataframes or create CSV files? (print/csv): ").strip().lower()

    if choice == 'print':
        # If user chooses to print, iterate through the DataFrames and print them
        for table_name, df in result_dataframes.items():
            print(f"\n{table_name}:")
            print(df)
            print("\n" + "=" * 50)
    
    elif choice == 'csv':
        # Create an output directory for CSV files if it doesn't exist
        output_dir = os.path.join(directory, 'outputs')
        os.makedirs(output_dir, exist_ok=True)
        
        # Iterate through the DataFrames and save each as a CSV file
        for table_name, df in result_dataframes.items():
            csv_file_path = os.path.join(output_dir, f"{table_name}.csv")
            df.to_csv(csv_file_path, index=False)  # Write DataFrame to CSV
            print(f"CSV for {table_name} created at: {csv_file_path}")
    
    else:
        # Inform the user if the input is invalid
        print("Invalid choice. Please enter 'print' or 'csv'.")
