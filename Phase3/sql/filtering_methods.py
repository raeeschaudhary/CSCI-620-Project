from globals import data_directory, cleaned_files
import pandas as pd


def clean_csv_columns_to_keep(csv_file, output_file, columns_to_keep):
    """
    Takes a input csv file and removes the columns not provided and stores the cleaned file.
    
    :param csv_file: Name to input csv file.
    :param output_file: Name to output csv file.
    :param columns_to_keep: List of columns to keep in the output csv file.
    """
    # combine the csv_file and output_file, with data_directory given in the globals.py
    input_file = data_directory + csv_file
    output_file = data_directory + output_file
    # Read the entire CSV file into a DataFrame
    df = pd.read_csv(input_file)
    # Filter the DataFrame to keep only the specified columns
    cleaned_df = df[columns_to_keep]
    # Save the cleaned DataFrame to a new CSV file
    cleaned_df.to_csv(output_file, index=False)

def convert_columns_to_int(csv_file, output_file, columns_to_convert):
    """
    Takes a input csv file and removes the columns not provided and stores the cleaned file.
    
    :param csv_file: Name to input csv file.
    :param output_file: Name to output csv file.
    :param column_convert: The column to change data type to integer (parsing).
    """
    # combine the csv_file and output_file, with data_directory given in the globals.py
    input_file = data_directory + csv_file
    output_file = data_directory + output_file
    # Read the entire CSV file into a DataFrame
    df = pd.read_csv(input_file)
    # check if the column to convert exists in the DataFrame
    if columns_to_convert in df.columns:
        df[columns_to_convert] = pd.to_numeric(df[columns_to_convert])
        # convert the column to numeric (keeping NaN as NaN) only convert non-numeric values to NaN
        df[columns_to_convert] = df[columns_to_convert].astype('Int64')
    # save the cleaned DataFrame to a new CSV file
    df.to_csv(output_file, index=False)

def check_print_duplicates():
    """
    Checks duplicates and prints the count of duplicates on filtered files.
    """
    for file in cleaned_files:
        df =  pd.read_csv(data_directory + file + '.csv')
        duplicates = df[df.duplicated()]
        print(f'file: {file}.csv, duplicates: {len(duplicates)}')
