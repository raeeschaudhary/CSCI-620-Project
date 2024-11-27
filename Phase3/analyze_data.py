from globals import *
import pandas as pd

def analyze_csv(file_path, chunksize=10000, field_name=None):
    """
    Analyzes each chunk of the CSV file to check the values, data types, max_length, null count, and unique count for each attribute.
    If field_name is provided, it will also print rows where that field is null.
    
    :param file_path: Path to input CSV file.
    :param chunksize: Chunk size for reading CSV file.
    :param field_name: Field name to check for nulls and print rows with null values in that field.
    :return: key_info (count, length, nulls, uniques) and count of total records.
    """
    # to store key info
    key_info = {}
    # to track total records processed
    total_records = 0
    
    try:
        # Read CSV file in chunks
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            # Increment total records by the number of rows in the chunk
            total_records += chunk.shape[0]
            
            # If field_name is provided, check for rows where the field is null
            if field_name:
                null_rows = chunk[chunk[field_name].isna()]
                if not null_rows.empty:
                    print(f"Rows where {field_name} is null:")
                    print(null_rows)
            
            # Process each column in the chunk
            for column in chunk.columns:
                # Drop NaNs and convert to string
                temp_values = chunk[column].dropna().astype(str)
                # Get length of values
                value_length = temp_values.str.len()
                
                # Initialize key_info for the column if not already done
                if column not in key_info:
                    key_info[column] = {
                        "longest_value": temp_values[value_length.idxmax()] if not temp_values.empty else '',
                        "longest_value_length": value_length.max() if not temp_values.empty else 0,
                        "null_count": chunk[column].isna().sum(),
                        "unique_count": len(temp_values.unique())
                    }
                else:
                    # Update longest_value, and longest_value_length
                    current_max_length = key_info[column]["longest_value_length"]
                    new_max_length = value_length.max()
                    if new_max_length > current_max_length:
                        key_info[column]["longest_value"] = temp_values[value_length.idxmax()]
                        key_info[column]["longest_value_length"] = new_max_length
                    # Update null count
                    key_info[column]["null_count"] += chunk[column].isna().sum()
                    # Update unique count
                    unique_values = temp_values.unique()
                    key_info[column]["unique_count"] = min(key_info[column]["unique_count"] + len(unique_values), 50)

    except Exception as e:
        # If there is an error reading file; printing it
        print(f"Error processing file {file_path}: {e}")
    
    return key_info, total_records


def save_results_to_file(file_paths, output_file, field_name=None):
    """
    Saves the exploration out to a text file for further manual analysis
    
    :param file_paths: List of input file names.
    :param output_file: Output file name to store results.
    :param field_name: Field name to check for nulls and print rows with null values in that field.
    """
    # Open the output file in write mode
    with open(output_file, 'w') as f:
        # Process each file in the input_files
        for file in file_paths:
            # Combine the file path with data_directory given in globals.py plus extension to .csv
            file_path = data_directory + file + ".csv"
            print("Exploring: ", file_path)
            f.write("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
            f.write(f"File: {file_path}\n")
            # Call the CSV analysis function
            key_info, total_records = analyze_csv(file_path, field_name=field_name)
            # Write the file info and total records
            f.write(f"Total Records: {total_records}\n")
            f.write("Key information with max value lengths, longest values, and null counts:\n")
            for key, info in sorted(key_info.items()):
                f.write(f"Key: {key}\n")
                f.write(f"Longest Value Length: {info['longest_value_length']}\n")
                # Check if the longest value length exceeds 1000 characters
                if info['longest_value_length'] > 1000:
                    f.write(f"Longest Value: {info['longest_value']} (Greater than 1000 characters)\n")
                else:
                    f.write(f"Longest Value: {info['longest_value']}\n")
                f.write(f"Null count: {info['null_count']}\n")
                f.write(f"Unique Count: {info['unique_count']}\n")
            f.write("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
            f.write("\n")


if __name__ == "__main__":   
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Exploring Files")
    # Name of the output file to store results of analysis
    output_file = 'file_info.txt'
    # Specify the field name to check for null values (e.g., 'username')
    field_name = 'SubmittedUserId'
    # Call the method to explore files.
    save_results_to_file(['SubmissionsCleaned'], output_file, field_name=field_name)
    print('File Results Stored to ', output_file)
    print('++++++++++++++++++++++++++++++++++++++++++++++')
