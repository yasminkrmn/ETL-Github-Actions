# Import necessary libraries
import pandas as pd  # For data manipulation and analysis
import datetime  # For working with date and time
import requests  # For making HTTP requests

def day_offset(start, end, offset):
    """
    Generates a list of dates at specified intervals (in days) from start to end.

    Parameters:
    start (datetime.date): The starting date.
    end (datetime.date): The ending date.
    offset (int): The number of days to increment for each entry in the list.

    Returns:
    list: A list of dates from start to end, incremented by the specified offset.
    """
    # Initialize a list with the starting date
    current = [start]

    # Loop until the last date in the current list is less than the end date
    while max(current) < end:
        # Check if adding the offset to the last date will not exceed the end date
        if (max(current) + datetime.timedelta(days=offset) < end):
            # If it doesn't exceed, append the new date to the list
            current.append(max(current) + datetime.timedelta(days=offset))
        else:
            # If it exceeds, append the end date to the list
            current.append(end)

    # Return the list of dates
    return current


def hour_offset(start, end, offset):
    """
    Generates a list of datetime objects at specified intervals (in hours) from start to end.

    Parameters:
    start (datetime.datetime): The starting datetime.
    end (datetime.datetime): The ending datetime.
    offset (int): The number of hours to increment for each entry in the list.

    Returns:
    list: A list of datetime objects from start to end, incremented by the specified offset.
    """
    # Initialize a list with the starting datetime
    current = [start]

    # Loop until the last datetime in the current list is less than the end datetime
    while max(current) < end:
        # Check if adding the offset to the last datetime will not exceed the end datetime
        if (max(current) + datetime.timedelta(hours=offset) < end):
            # If it doesn't exceed, append the new datetime to the list
            current.append(max(current) + datetime.timedelta(hours=offset))
        else:
            # If it exceeds, append the end datetime to the list
            current.append(end)

    # Return the list of datetime objects
    return current


def eia_get(api_key,
            api_path,
            data="value",
            facets=None,
            start=None,
            end=None,
            length=None,
            offset=None,
            frequency=None):
    """
    Fetches data from the EIA API based on specified parameters.

    Parameters:
    api_key (str): The API key for authentication.
    api_path (str): The path to the specific API endpoint.
    data (str): The data to fetch, default is "value".
    facets (dict): Additional filtering options for the API request.
    start (datetime): The start date for the data request.
    end (datetime): The end date for the data request.
    length (int): The number of data points to return.
    offset (int): The number of data points to skip.
    frequency (str): The frequency of the data (e.g., daily, monthly).

    Returns:
    response: An object containing the fetched data, URL used, and parameters.
    """

    # Inner class to structure the response from the API
    class response:
        def __init__(output, data, url, parameters):
            output.data = data
            output.url = url
            output.parameters = parameters

    # Validate the API key
    if type(api_key) is not str:
        print("Error: The api_key argument is not a valid string")
        return
    elif len(api_key) != 40:
        print("Error: The length of the api_key is not valid, must be 40 characters")
        return

    # Ensure the API path ends with a "/"
    if api_path[-1] != "/":
        api_path = api_path + "/"

    # Build the facets part of the URL if any facets are provided
    if facets is None:
        fc = ""
    else:
        fc = ""
    for i in facets.keys():
        if type(facets[i]) is list:
            for n in facets[i]:
                fc = fc + "&facets[" + i + "][]=" + n
        elif type(facets[i]) is str:
            fc = fc + "&facets[" + i + "][]=" + facets[i]

    # Build the start date part of the URL if provided
    if start is None:
        s = ""
    else:
        if type(start) is datetime.date:
            s = "&start=" + start.strftime("%Y-%m-%d")
        elif type(start) is datetime.datetime:
            s = "&start=" + start.strftime("%Y-%m-%dT%H")
        else:
            print("Error: The start argument is not a valid date or time object")
            return

    # Build the end date part of the URL if provided
    if end is None:
        e = ""
    else:
        if type(end) is datetime.date:
            e = "&end=" + end.strftime("%Y-%m-%d")
        elif type(end) is datetime.datetime:
            e = "&end=" + end.strftime("%Y-%m-%dT%H")
        else:
            print("Error: The end argument is not a valid date or time object")
            return

    # Build the length part of the URL if provided
    if length is None:
        l = ""
    else:
        l = "&length=" + str(length)

    # Build the offset part of the URL if provided
    if offset is None:
        o = ""
    else:
        o = "&offset=" + str(offset)

    # Build the frequency part of the URL if provided
    if frequency is None:
        fr = ""
    else:
        fr = "&frequency=" + str(frequency)

    # Construct the full API URL
    url = "https://api.eia.gov/v2/" + api_path + "?data[]=value" + fc + s + e + l + o + fr

    # Send the GET request to the API and parse the JSON response
    d = requests.get(url + "&api_key=" + api_key).json()

    # Check the API response for validity
    if 'response' not in d or 'data' not in d['response'] or not d['response']['data']:
        print("Error: No valid data returned from API")
        return response(data=pd.DataFrame(), url=url + "&api_key=", parameters={})

    # Create a DataFrame from the response data
    df = pd.DataFrame(d['response']['data'])

    # Print the columns of the DataFrame for verification
    print("DataFrame Columns:", df.columns)

    # Reformat the output DataFrame
    if 'period' in df.columns:
        df["period"] = pd.to_datetime(df["period"])
    else:
        print("Warning: 'period' column not found in DataFrame")

    if 'value' in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors='coerce')
    else:
        print("Warning: 'value' column not found in DataFrame")

    # Sort the DataFrame by the 'period' column
    df = df.sort_values(by=["period"])

    # Prepare the parameters for the response object
    parameters = {
        "api_path": api_path,
        "data": data,
        "facets": facets,
        "start": start,
        "end": end,
        "length": length,
        "offset": offset,
        "frequency": frequency
    }

    # Create a response object to return
    output = response(data=df, url=url + "&api_key=", parameters=parameters)
    return output


def eia_backfill(start, end, offset, api_key, api_path, facets):
    """
    Fetches backfilled data from the EIA API for specified date ranges.

    Parameters:
    start (datetime): The start date for the data request.
    end (datetime): The end date for the data request.
    offset (int): The number of days or hours to increment for each request.
    api_key (str): The API key for authentication.
    api_path (str): The path to the specific API endpoint.
    facets (dict): Additional filtering options for the API request.

    Returns:
    response: An object containing the fetched data and parameters.
    """

    # Inner class to structure the response from the API
    class response:
        def __init__(output, data, parameters):
            output.data = data
            output.parameters = parameters

    print("eia_backfill function started.")

    # Validate the API key
    if type(api_key) is not str:
        print("Error: The api_key argument is not a valid string")
        return
    elif len(api_key) != 40:
        print("Error: The length of the api_key is not valid, must be 40 characters")
        return

    # Ensure the API path ends with a "/"
    if api_path[-1] != "/":
        api_path = api_path + "/"

    # Check the start date type and format it for the API request
    if isinstance(start, datetime.date):
        s = "&start=" + start.strftime("%Y-%m-%d")
    elif isinstance(start, datetime.datetime):
        s = "&start=" + start.strftime("%Y-%m-%dT%H")
    else:
        print("Error: The start argument is not a valid date or time object")
        return

    # Check the end date type and format it for the API request
    if isinstance(end, datetime.date):
        e = "&end=" + end.strftime("%Y-%m-%d")
    elif isinstance(end, datetime.datetime):
        e = "&end=" + end.strftime("%Y-%m-%dT%H")
    else:
        print("Error: The end argument is not a valid date or time object")
        return

    # Create a time series based on the start and end dates
    try:
        if isinstance(start, datetime.date):
            time_vec_seq = day_offset(start=start, end=end, offset=offset)
        elif isinstance(start, datetime.datetime):
            time_vec_seq = hour_offset(start=start, end=end, offset=offset)

        print(f"Time series created: {time_vec_seq}")
    except Exception as e:
        print(f"Error occurred while creating the time series: {e}")
        return

    # Loop through each time interval to fetch data
    dfs = []  # Initialize an empty list to hold DataFrames
    for i in range(len(time_vec_seq[:-1])):
        start = time_vec_seq[i]  # Set the current start date
        if i < len(time_vec_seq[:-1]) - 1:
            end = time_vec_seq[i + 1] - datetime.timedelta(hours=1)  # End is the next start minus one hour
        elif i == len(time_vec_seq[:-1]) - 1:
            end = time_vec_seq[i + 1]  # Last end date

        print(f"Fetching data: start: {start}, end: {end}")

        # Fetch data from the API
        try:
            temp = eia_get(api_key=api_key,
                           api_path=api_path,
                           facets=facets,
                           start=start,
                           data="value",
                           end=end)

            # Check if the returned DataFrame is empty
            if temp.data.empty:
                print(f"No data returned for start: {start}, end: {end}")
                continue  # Skip to the next iteration

            # Check for the presence of 'period' and 'value' columns
            if 'period' not in temp.data.columns or 'value' not in temp.data.columns:
                print("Warning: 'period' or 'value' columns not found!")
                continue

            # Attempt to convert the 'value' column to numeric
            try:
                temp.data["value"] = pd.to_numeric(temp.data["value"])
            except Exception as e:
                print(f"Error converting 'value' to numeric: {e}")
                continue

            # Append the DataFrame to the list
            dfs.append(temp.data)  # Store the results in the list

            # Print a sample of the returned data
            # print("Sample of returned data:\n", temp.data.head())  # Show the first few rows of data

        except Exception as e:
            print(f"Error occurred while fetching data from API: {e}")
            continue  # Skip to the next iteration

    # Concatenate all DataFrames into one
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        # print("Error occurred while concatenating data. Sample of concatenated data:\n", df.head())
    else:
        df = pd.DataFrame()  # Create an empty DataFrame if no DataFrames are available
        print("No DataFrames to concatenate, returning empty DataFrame.")

    # Prepare the parameters for the response object
    parameters = {
        "api_path": api_path,
        "data": "value",
        "facets": facets,
        "start": start,
        "end": end,
        "length": None,
        "offset": offset,
        "frequency": None
    }

    print("Data fetching completed. Number of records fetched:", len(df))
    output = response(data=df, parameters=parameters)
    print("eia_backfill function completed.")

    return output

def eia_metadata(api_key, api_path=None):
    """
    Retrieves metadata from the EIA API.

    Parameters:
    api_key (str): The API key for authentication.
    api_path (str, optional): The specific API endpoint path. Defaults to None.

    Returns:
    response: An object containing metadata, the URL used for the request, and parameters.
    """

    # Inner class to structure the response from the API
    class response:
        def __init__(output, meta, url, parameters):
            output.meta = meta  # Metadata from the API response
            output.url = url  # The URL that was requested
            output.parameters = parameters  # Parameters used for the request

    # Validate the API key
    if type(api_key) is not str:
        print("Error: The api_key argument is not a valid string")
        return
    elif len(api_key) != 40:
        print("Error: The length of the api_key is not valid, must be 40 characters")
        return

    # Construct the base URL based on the provided api_path
    if api_path is None:
        url = "https://api.eia.gov/v2/" + "?api_key="  # Base URL when no api_path is provided
    else:
        if api_path[-1] != "/":
            api_path = api_path + "/"  # Ensure the api_path ends with a "/"
        url = "https://api.eia.gov/v2/" + api_path + "?api_key="  # Full URL with api_path

    # Send a GET request to the constructed URL and parse the JSON response
    d = requests.get(url + api_key).json()

    # Prepare the parameters for the response object
    parameters = {
        "api_path": api_path  # Include api_path in the parameters
    }

    # Create an instance of the response class with the API data
    output = response(url=url, meta=d["response"], parameters=parameters)

    return output  # Return the structured response object
