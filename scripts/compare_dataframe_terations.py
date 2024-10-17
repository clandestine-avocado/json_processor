import pandas as pd

# Create a sample dictionary of DataFrames
sample_dataframes = {
    'df1': pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']}),
    'df2': pd.DataFrame({'X': [4, 5, 6], 'Y': ['d', 'e', 'f']})
}

print("Your version (for value in dataframes.items()):")
for value in sample_dataframes.items():
    print(f"Type of 'df': {type(value)}")
    print(f"Contents of 'df': {value}")
    print("---")

print("\nCorrected version (for key, value in dataframes.items()):")
for key, value in sample_dataframes.items():
    print(f"Type of 'name': {type(key)}")
    print(f"Contents of 'name': {key}")
    print(f"Type of 'df': {type(value)}")
    print(f"Contents of 'df':\n{value}")
    print("---")