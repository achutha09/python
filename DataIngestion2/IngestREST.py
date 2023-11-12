# This code file should contain API calls required to get the statewide CO2 emissions for 2019.
# The problem should be able to run by the instructor using 'python ingestREST.py' (with their own API key).
# Store your key in a file with a .config appendix to avoid accidental check-in (.gitignore is setup to ignore .config)

# Assignment problems:
#   1a) Code the REST API to get the 2019 state level total CO2 emissions from https://www.eia.gov/.
#       Hint:  Use https://www.eia.gov/opendata/browser/ to develop your query.
#       Hint:  You'll want to set the sectorID to 'TT' (Total carbon dioxide emissions from all sectors) 
#               and the fuel ID to 'TO' (All fuels)
#   1b) Save the data to a CSV file to 'data\CO2\TotalEmissionsByState2019.csv'.
#   1c) Answer the first question in 'data\CO2\NDOverallRank.txt'
#
#   2a) Code the REST API to get the 2019 state level total CO2 emissions for Coal.
#   2b) Save the data to a CSV file to 'data\CO2\CoalEmissionsByState2019.csv'.
#   2c) Answer the second question in 'data\CO2\NDCoalRank.txt'
#
#   3) Answer the third question in data\CO2.

# Points for this file:
#   - Part 1:  8 points
#   - Part 2:  8 points
#   - Part 3:  4 points

print("IngestREST problem")