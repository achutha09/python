import kaggle
import pandas

# Authentication
kaggle.api.authenticate()

# Identifier for the worlds population dataset on Kaggle
world_population_dataset_name = "iamsouravbanerjee/world-population-dataset"

# Destination path for downloading the csv file
csv_file_destination_path = "Data/WorldPopulation"

# Kaggle API call to download the csv file to the csv_destination_path
kaggle.api.dataset_download_files(world_population_dataset_name, csv_file_destination_path, unzip=True)

# Source path for the csv file to read
csv_file_path = "Data/WorldPopulation/world_population.csv"

# Reading the csv file using pandas library dataframe
df = pandas.read_csv(csv_file_path)

# Check for the columns 2000 Population and 2022 Population to fetch the details and calculate increase in population
if "2000 Population" in df.columns and "2022 Population" in df.columns:
    df["PercentIncrease_2000_2022"] = (df["2022 Population"] - df["2000 Population"]) / df["2000 Population"]
else:
    print("Column names not found. Please adjust the column names in your CSV file.")

# Sort the data by the percent increase in descending order
df = df.sort_values(by="PercentIncrease_2000_2022", ascending=False)

# Define the path to save the new file
output_file_path = "Data/WorldPopulation/PopulationWithPercentIncrease.csv"

# Save the DataFrame to a new CSV file
df.to_csv(output_file_path, index=False)

print("CSV file with percent increase saved successfully to " + output_file_path)
