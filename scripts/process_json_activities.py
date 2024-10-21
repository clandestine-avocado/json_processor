import pandas as pd
import re
import os

def process_activities(csv_file):
    # Read the CSV file
    df = pd.read_csv(csv_file, sep="|")
    
    # Convert 'value' column to string datatype with error handling
    try:
        df['value'] = df['value'].astype(str)
    except Exception as e:
        print(f"Warning: Failed to convert 'value' column to string. Error: {e}")
        print("Proceeding with original data types.")
    
    # Function to process each value in the 'value' column
    def process_value(value):
        # If value is not a string, return original value
        if not isinstance(value, str):
            return pd.Series({'ActivityType': None, 'value': value})
        
        # Remove tabs
        value = re.sub(r'\t+', '', value)
        
        # If value contains "N/A" after removing tabs, return None for both columns
        if "N/A" in value:
            return pd.Series({'ActivityType': None, 'value': None})
        
        try:
            # Use regex to extract the activity type and value
            match = re.search(r"'type':\s*'([^']*)'.*'activity':\s*'([^']*)'", value)
            
            if match:
                activity_type = match.group(1).strip()
                new_value = match.group(2).strip()
                
                # If activity_type is empty, return None for both columns
                if not activity_type:
                    return pd.Series({'ActivityType': None, 'value': None})
                
                return pd.Series({'ActivityType': activity_type, 'value': new_value})
            else:
                # If the pattern doesn't match, return None for both columns
                return pd.Series({'ActivityType': None, 'value': None})
        
        except Exception:
            # If any error occurs during processing, return None for both columns
            return pd.Series({'ActivityType': None, 'value': None})
    
    # Apply the processing function to the 'value' column
    processed = df['value'].apply(process_value)
    
    # Update the DataFrame
    df['ActivityType'] = processed['ActivityType']
    df['value'] = processed['value']
    
    # Drop rows where ActivityType is None (indicating no processing occurred or N/A was found)
    df = df.dropna(subset=['ActivityType'])
    
    # Generate output filename
    base_name = os.path.splitext(csv_file)[0]
    output_file = f"{base_name}_processed.csv"
    
    # Write the processed DataFrame to a new CSV file
    df.to_csv(output_file, sep='|', index=False)
    
    return df

# Usage
csv_file = "myCSV.csv"
result_df = process_activities(csv_file)
print(result_df)
print(f"Processed data written to: {os.path.splitext(csv_file)[0]}_processed.csv")