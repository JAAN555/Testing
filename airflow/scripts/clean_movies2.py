import pandas as pd
import sys
import csv
import os
# Increase CSV field size limit
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)

def clean_and_split_genres(input_file, output_file):
    # Load the dataset
    df = pd.read_csv(
        input_file, 
        encoding='utf-8',
        delimiter=',',
        on_bad_lines='skip',  # Skips rows that cause parsing errors
        engine='python',      # Use the Python parser for more flexibility
    )

    # Ensure 'Genre' column exists
    if 'Genre' not in df.columns:
        raise ValueError("The dataset does not have a 'Genre' column.")

    # Split the 'Genre' column into individual genres and find unique genres
    genres = df['Genre'].dropna().str.split(', ').explode().unique()

    # Create a new binary column for each genre
    for genre in genres:
        df[genre] = df['Genre'].apply(
            lambda x: 1 if isinstance(x, str) and genre in x.split(', ') else 0
        )

    # Drop the original 'Genre' column
    df.drop(columns=['Genre'], inplace=True)

    # Save the cleaned dataset to a new CSV file
    df.to_csv(output_file, index=False)
    print(f"Cleaned dataset saved to {output_file}")

script_dir = os.path.dirname(os.path.abspath(__file__))

# File paths

input_file = os.path.join(script_dir, '../data/movies/mymoviedb_raw.csv')
output_file = os.path.join(script_dir, '../data/movies/cleaned_mymoviedb.csv')

# Execute the function
clean_and_split_genres(input_file, output_file)
