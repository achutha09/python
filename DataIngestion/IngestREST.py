import configparser
import requests
import csv

# The api key is mentioned in api_key.config file
file_path = "api_key.config"


def construct_api_url(base_url, state_ids):
    """Construct the API endpoint URL with state IDs as query parameters.
    Args:
        base_url (str): The base URL of the API.
        state_ids (list): A list of state IDs.
    Returns:
        str: The constructed API endpoint URL."""
    query_params = [f'facets[stateId][]={state}' for state in state_ids]
    return f"{base_url}?{'&'.join(query_params)}"


def download_data_from_api(url, params):
    """Download data from the API.
    Args:
        url (str): The API endpoint URL.
        params (dict): Query parameters for the API request.
    Returns:
        dict: JSON response data."""
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f'Failed to retrieve data. Status code: {response.status_code}')


def save_data_to_csv(data, csv_destination_path):
    """This method saves data to a CSV file:
       takes parameters data and destination
       data : Data to be saved.
       csv_destination_path : The destination path to the CSV file."""
    with open(csv_destination_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # header row
        header = [
            "Period",
            "Sector ID",
            "Sector Name",
            "Fuel ID",
            "Fuel Name",
            "State ID",
            "State Name",
            "Value",
            "Value Units"
        ]
        csv_writer.writerow(header)

        # Loop to iterate over the response and write each entry as a single record
        for entry in data['response']['data']:
            row = [
                entry['period'],
                entry['sectorId'],
                entry['sector-name'],
                entry['fuelId'],
                entry['fuel-name'],
                entry['stateId'],
                entry['state-name'],
                entry['value'],
                entry['value-units']
            ]
            csv_writer.writerow(row)


def download_data_to_csv(params, state_ids, csv_destination_path):
    # Defining the base URL of the api
    base_url = "https://api.eia.gov/v2/co2-emissions/co2-emissions-aggregates/data/"

    # Construct the final URL based on the parameters
    url = construct_api_url(base_url, state_ids)

    # Download data from the API
    data = download_data_from_api(url, params)

    # Save the data to a CSV file
    save_data_to_csv(data, csv_destination_path)

    print(f'Data saved to {csv_destination_path}')


def read_api_key(file_path):
    try:
        config = configparser.ConfigParser()
        config.read(file_path)

        # Check if the 'API' section exists in the file
        if 'API' in config:
            # Check if the 'API_KEY' key exists within the 'API' section
            if 'API_KEY' in config['API']:
                return config['API']['API_KEY']

        raise KeyError('API_KEY not found in the configuration file.')
    except FileNotFoundError:
        raise Exception('API key configuration file not found.')


# Parameters for Total emission by state in year 2019
paramsTotalEmissionByState2019 = {
    'api_key': read_api_key(file_path),
    'frequency': 'annual',
    'data[0]': 'value',
    'facets[sectorId][]': 'TT',
    'facets[fuelId][]': 'TO',
    'start': '2019',
    'end': '2019',
    'offset': '0',
    'length': '5000'
}

# Parameters for coal emission by state in year 2019
paramsCoal = {
    'api_key': read_api_key(file_path),
    'frequency': 'annual',
    'data[0]': 'value',
    'facets[sectorId][]': 'TT',
    'facets[fuelId][]': 'CO',
    'start': '2019',
    'end': '2019',
    'offset': '0',
    'length': '5000'
}

state_ids = [
    'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL',
    'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA',
    'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE',
    'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI',
    'SC', 'SD', 'TN', 'TX', 'US', 'UT', 'VA', 'VT', 'WA', 'WI',
    'WV', 'WY'
]

csv_destination_path_TotalEmissionsByState2019 = "Data/CO2/TotalEmissionsByState2019.csv"
csv_destination_path_CoalEmissionsByState2019 = "Data/CO2/CoalEmissionsByState2019.csv"
read_api_key(file_path)
download_data_to_csv(paramsTotalEmissionByState2019, state_ids, csv_destination_path_TotalEmissionsByState2019)
download_data_to_csv(paramsCoal, state_ids, csv_destination_path_CoalEmissionsByState2019)
