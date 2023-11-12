# This code file should contain API calls required to get the Kaggle dataset for world population.
# The problem should be able to run by the instructor using 'python IngestKaggle.py' (with their own API key).

# Assignment problems:
#   1a) Use the Kaggle Python library to retrieve the World Population Dataset from Kaggle.com's Datasets.
#       - This is the dataset - https://www.kaggle.com/datasets/iamsouravbanerjee/world-population-dataset.
#   1b) Download the population data file to 'data\WorldPopulation'.
#   

#   2a) Read the CSV file using Pandas.
#   2b) Calculate the % increase in population from 2000 to 2022 and create a new column named "PercentIncrease_2000_2022" with that value.
#           Note: store as a decimal - a 10% increase should be stored as 0.10.
#   2c) Sort the dataframe descending on "PercentIncrease_2000_2022".
#   2d) Save the new file 'data\WorldPopulation\PopulationWithPercentIncrease.csv'

#   3) Answer the questions in 'data\CO2\WorldPopulation.csv'.  Keep the formatting consistent by replacing 
#       "Your*" with the appropriate information.  Opening in Excel will make this easy.

# Points for this file are the following.  Beside being runnable and correct, the code should be well structured
# with descriptive variables, commented as appropriate, and without extra code commented out.
#   - Part 1:  8 points
#   - Part 2:  8 points
#   - Part 3:  4 points

print("IngestKaggle problem")