import kagglehub
import shutil
import os
import pandas as pd
import sys
import schedule
import time
import re

# Specify the save path
save_path = "C:/Users/Jerico/Documents/gitRepositories/Dirty-E-Commerce-Data-Project/source_files"


def download_dataset():
    """Function to download the dataset using KaggleHub."""
    path = kagglehub.dataset_download("oleksiimartusiuk/e-commerce-data-shein")
    print("Path to dataset files:", path)
    return path


def move_files(path):
    """Function to move downloaded files to the specified save path."""
    try:
        if os.path.exists(save_path):
            shutil.rmtree(save_path)
            print(f"Existing files at {save_path} removed.")

        # Move the downloaded files to the specified path
        if os.path.exists(path):
            shutil.move(path, save_path)
            print(f"Dataset moved to {save_path}")
        else:
            print("Download path not found!")
    except FileNotFoundError as e:
        print(f"Error: The specified file or directory was not found: {e}")


def load_all_csv_files(folder_path):
    """Function to load all CSV files in the given folder into a dictionary of DataFrames."""
    dataframes = {}
    # Get only the csv files
    for filename in os.listdir(folder_path):
        if filename.endswith("csv"):
            file_path = os.path.join(folder_path, filename)

            # filename as dataframe name
            df_name = filename.replace(".csv", "")

            # Read the csv
            dataframes[df_name] = pd.read_csv(file_path)

            # Print the first row in the dataframe to confirm
            print(f"Data from {df_name}: \n", dataframes[df_name].head(1))

    return dataframes


def remove_columns(df: pd.DataFrame, excluded_columns: list) -> pd.DataFrame:
    """Function to remove the excluded columns for each dataframe"""
    # Get all the columns:
    for column in df.columns:
        # Get the column names
        if column in excluded_columns:
            # Drop the column
            df.drop(column, axis=1, inplace=True)
            print(f"Column '{column}' dropped.")
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Function to clean the dataframe"""
    # Drop rows where all values are NaN
    df = df.dropna(how="all")

    # Fill NaN values with 0
    df = df.fillna(0)

    # Remove the dollar sign from the 'price' column and convert to float
    df["price"] = df["price"].str.replace(r"\$", "", regex=True)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Clean and convert the 'discount' column to integer
    df["discount"] = (
        df["discount"]
        .str.replace("-", "", regex=False)
        .str.replace("%", "", regex=False)
        .fillna(0)
        .astype(int)
    )

    return df


def convert_to_numeric(selling_str):
    """Function to convert selling proposition to numeric"""
    selling_str = str(selling_str)

    # Convert k+ to numeric (thousands)
    if "k+" in selling_str:
        number = float(re.sub(r"[^\d\.]", "", selling_str))
        return number * 1000

    # Convert m+ to numeric (millions)
    elif "m+" in selling_str:
        number = float(re.sub(r"[^\d\.]", "", selling_str))
        return number * 1000000

    # Convert + to numeric (just the number, e.g., 400+ -> 400)
    elif "+" in selling_str:
        number = float(re.sub(r"[^\d\.]", "", selling_str))
        return number

    return None


def export_to_csv(df: pd.DataFrame, export_path: str):
    """Exports the dataframe to the specified CSV path.
    Function to export the combined dataframe to a CSV"""
    # Ensure the exports directory exists
    os.makedirs(os.path.dirname(export_path), exist_ok=True)

    # Export to CSV
    df.to_csv(export_path, index=False)
    print(f"Dataframe exported to {export_path}")


def run_pipeline():
    """Function that runs the entire pipeline."""
    print("Starting pipeline...")

    # Step 1: Download dataset
    dataset_path = download_dataset()

    # Step 2: Move files to save path
    move_files(dataset_path)

    # Step 3: Load all CSV files into DataFrames
    dataframes = load_all_csv_files(save_path)

    # Step 4: Remove excluded columns from each DataFrame
    # Define the excluded columns
    excluded_columns = [
        "goods-title-link--jump href",
        "goods-title-link--jump",
        "rank-title",
        "rank-sub",
        "color-count",
        "blackfridaybelts-bg src",
        "blackfridaybelts-content",
        "product-locatelabels-img src",
    ]

    # Clean the data by removing excluded columns
    for df_name in dataframes:
        df = dataframes[df_name]
        print(f"Cleaning {df_name}")

        # Use function to remove excluded columns
        df_cleaned = remove_columns(df, excluded_columns)

        # Further clean the dataframe (e.g., remove NaNs, handle price and discount)
        df_cleaned = clean_dataframe(df_cleaned)

        # Update the dataframe with the cleaned version
        dataframes[df_name] = df_cleaned

    # Print out cleaned dataframes
    for df_name in dataframes:
        print(f"\n DataFrame {df_name}")
        print(dataframes[df_name].head())

    # Combine all dataframes into one
    combined_df = pd.concat(dataframes.values(), ignore_index=True)
    print("Combined dataframe shape:", combined_df.shape)

    # Step 5: Apply conversion function to 'selling_proposition' and clean data
    combined_df["order_quantity"] = combined_df["selling_proposition"].apply(
        convert_to_numeric
    )
    combined_df = combined_df.drop("selling_proposition", axis=1)
    combined_df.rename(columns={"goods-title-link": "product_name"}, inplace=True)

    # Export the cleaned and combined dataframe to CSV
    export_to_csv(combined_df, "./exports/after_cleaning.csv")

    return combined_df


def schedule_pipeline():
    """Function to run the pipeline based on a schedule."""
    print("Scheduling pipeline...")
    schedule.every().day.at("14:59").do(run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(1)


def main():
    """Main function to control the execution flow."""
    if len(sys.argv) > 1:
        param = sys.argv[1]
    else:
        print("Please provide an argument (manual/schedule).")
        return

    if param == "manual":
        # Run the pipeline manually
        run_pipeline()

    elif param == "schedule":
        # Run the pipeline based on schedule
        schedule_pipeline()

    else:
        print("Invalid parameter for app.py.")


if __name__ == "__main__":
    main()
