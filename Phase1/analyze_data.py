from funcs.globals import *
import pandas as pd

# This method analyzes each chunk of the CSV file to check the values, data types, max_length, null count, and unique count for each attribute.
def analyze_csv(file_path, chunksize=10000):
    key_info = {}
    total_records = 0
    
    try:
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            total_records += chunk.shape[0]  # Increment total records by the number of rows in the chunk
            
            for column in chunk.columns:
                temp_values = chunk[column].dropna().astype(str)  # Drop NaNs and convert to string
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
                    # Update max_length, longest_value, and longest_value_length
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
        print(f"Error processing file {file_path}: {e}")

    return key_info, total_records

# This method saves the exploration out to a text file for further manual analysis
def save_results_to_file(file_paths, output_file):
    with open(output_file, 'w') as f:
        for file in file_paths:
            file_path = data_directory + file + ".csv"  # Change extension to .csv
            print("Exploring: ", file_path)
            f.write("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
            f.write(f"File: {file_path}\n")        
            key_info, total_records = analyze_csv(file_path)  # Call the CSV analysis function
            f.write(f"Total Records: {total_records}\n")
            f.write("Key information with max value lengths, longest values, and null counts:\n")  
            for key, info in sorted(key_info.items()):
                f.write(f"Key: {key}\n")
                f.write(f"Longest Value Length: {info['longest_value_length']}\n")
                
                # Check if the longest value length exceeds 1000
                if info['longest_value_length'] > 1000:
                    f.write(f"Longest Value: {info['longest_value']} (Greater than 1000 characters)\n")
                else:
                    f.write(f"Longest Value: {info['longest_value']}\n")

                f.write(f"Null count: {info['null_count']}\n")
                f.write(f"Unique Count: {info['unique_count']}\n")
                
            f.write("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
            f.write("\n")

if __name__=="__main__":   
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Exploring Files")
    
    output_file = 'file_info.txt'
    save_results_to_file(input_files, output_file)
    
    print('File Results Stored to ', output_file)
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
