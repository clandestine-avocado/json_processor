import pandas as pd
import re
import logging

def replace_whitespace_chars(dataframes):
    for name, df in dataframes.items():
        # Pattern to match one or more occurrences of \t, \n, \r, or \f
        pattern = r'[\t\n\r\f]+'
        
        # Apply the replacement to all string columns
        for column in df.select_dtypes(include=['object']):
            df[column] = df[column].replace(pattern, '', regex=True)
        
        logging.info(f"{name} whitespace characters replaced with zero length string")
    
    return dataframes

# Example usage
# Assuming 'dataframes' is a dictionary of DataFrames
# dataframes = {'df1': pd.DataFrame(...), 'df2': pd.DataFrame(...)}
# cleaned_dataframes = replace_whitespace_chars(dataframes)