import requests
from dotenv import load_dotenv
import json
import urllib.parse
import os
print("SCRIPT RUNNING")

# load variables
script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(script_dir, ".env") 

load_dotenv(dotenv_path,override=True)

keystone_url = os.getenv("KEYSTONE_URL")
gnocchi_url = os.getenv("GNOCCHI_URL")
nova_url = os.getenv("NOVA_URL")
es_name = os.getenv("ES_NAME")
es_password = os.getenv("ES_PASSWORD")
es_domain = os.getenv("ES_DOMAIN")
grain = os.getenv("GRANULARITY")
zoho_api_url = os.getenv("ZOHO_API_URL")


def get_openstack_token():
    auth_url = f"{keystone_url}/auth/tokens"
    headers = {
        'Content-Type': 'application/json'
    }

    auth_data = {
        "auth": {
        "identity": {
            "methods": [
                "password"
            ],
            "password": {

                "user": {

                    "domain": {

                        "name": es_domain

                    },

                    "name": es_name,

                    "password": es_password

                }

            }

        },

        "scope": {

            "domain": {

                "name": es_domain

                }

            }

        }

    }

    response = requests.post(auth_url, headers=headers, data=json.dumps(auth_data))

    token = response.headers['X-Subject-Token']

    return token


def get_project_details(token, project_id):

    """Fetch project details to get the domain ID for a given project."""

    auth_url = f"{keystone_url}/projects/{project_id}"  # Keystone endpoint

    headers = {

        'Content-Type': 'application/json',

        'X-Auth-Token': token

    }
    response = requests.get(auth_url, headers=headers)

    if response.status_code == 200:

        return response.json().get('project')

    else:

        return None


def get_domain_details(token, domain_id):

    url = f"{keystone_url}/domains/{domain_id}"

    headers = {

        'Content-Type': 'application/json',

        'X-Auth-Token': token

    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:

        return response.json().get('domain', {})

    return {}


def retrieve_data():

    token = get_openstack_token()

    print("Data retrieval running")


    # Gnocchi endpoint and token

    url = f"{gnocchi_url}/resource/instance"

    headers = {"X-Auth-Token": token, "Content-Type": "application/json"}


    # Specify the metrics you care about

    required_metrics = [

        "disk.write.requests.rate",

        "disk.read.bytes.rate",

        "cpu_util",

        "disk.write.bytes.rate",

        "memory.util",

        "memory.usage"

    ]


    # Fetch all instances

    response = requests.get(url, headers=headers)

    instances = response.json()


    id_to_name = {}


    # Filter metrics for each instance

    filtered_data = []

    for instance in instances:

        id_to_name[instance["id"]] = instance["resource_name"]

        instance_data = {"id": instance["id"], "metrics": {}}

        for metric_name, metric_id in instance["metrics"].items():

            if metric_name in required_metrics:

                instance_data["metrics"][metric_name] = metric_id

        filtered_data.append(instance_data)


    # Extract metrics for all instances

    for instance in filtered_data:

        for metric_name, metric_id in instance["metrics"].items():

            metric_url = f"{gnocchi_url}/metric/{metric_id}/measures"

            metric_response = requests.get(metric_url, headers=headers)

            measures = metric_response.json()

    

    # User-defined settings

    last_sync_file = f"{script_dir}/last_sync.txt"

    load_from_date = False  

    start_date = None

    latest_timestamp = None  # To keep track of the most recent timestamp


    # Check if the last_sync.txt file exists

    if os.path.exists(last_sync_file):

        with open(last_sync_file, "r") as file:

            start_date = file.read().strip()

            load_from_date = True

    print("start_date: ", start_date)

    print("load from date: ", load_from_date)


    # Gnocchi and Nova endpoints and token

    url = f"{gnocchi_url}/resource/instance"

    nova_api_url = f"{nova_url}/servers"

    headers = {"X-Auth-Token": token, "Content-Type": "application/json"}


    params = {'all_tenants':'true'}


    # Fetch all instances and map instance ID to its name

    nova_response = requests.get(nova_api_url, headers=headers,params=params) 

    instances_data = nova_response.json()


    response = requests.get(url, headers=headers)

    instances = response.json()

    output_data = []


    # Extract metrics for all instances

    for instance in instances:

        # print("INSTANCE: ",instance)

        instance_id = instance['id']

        instance_name = id_to_name.get(instance_id, 'unknown')

        tenant_id = instance.get('project_id')

        project_name = ''

        project_details = get_project_details(token, tenant_id)

        project_name = project_details.get('name')

        domain_id = project_details.get('domain_id', '')

        domain_details = get_domain_details(token, domain_id)

        domain_name = domain_details.get('name', '')

        timestamped_data = {}


        for metric_name, metric_id in instance["metrics"].items():

            if metric_name in required_metrics:

                # Build metric URL with optional date filtering

                metric_url = (

                    f"{gnocchi_url}/metric/{metric_id}/measures"

                )

                params={"granularity": 86400}

                if load_from_date and start_date:

                    params['start'] = start_date
                    params["granularity"] =  grain
                

                metric_response = requests.get(metric_url, headers=headers,params=params)


                # Max Aggregation Request

                max_params = params.copy()

                max_params["aggregation"] = "max"

                max_metric_response = requests.get(metric_url, headers=headers, params=max_params)


                # Min Aggregation Request

                min_params = params.copy()

                min_params["aggregation"] = "min"

                min_metric_response = requests.get(metric_url, headers=headers, params=min_params)


                if metric_response.status_code == 200 and max_metric_response.status_code == 200:

                    measures = metric_response.json()

                    max_measures = max_metric_response.json()  # Max aggregation

                    min_measures = min_metric_response.json()  # Min aggregation


                    max_values = {m[0]: m[2] for m in max_measures}  # Store max values by timestamp

                    min_values = {m[0]: m[2] for m in min_measures}  # Store max values by timestamp

                    # Organize data by timestamp

                    for measure in measures:

                        timestamp = measure[0]

                        granularity = measure[1]

                        value = measure[2]


                        # **Convert scientific notation to decimal format**

                        if isinstance(value, (float, int)):

                            value = float(f"{value:.15f}")

                            max_value = float(f"{max_values.get(timestamp, 0):.15f}")  # Get max value or default to 0

                            min_value = float(f"{min_values.get(timestamp, 0):.15f}")  # Get min value or default to 0


                        # Split timestamp into date and time

                        date_part, time_part = timestamp.split("T")

                        time_part = time_part.split("+")[0]  # Remove the timezone offset if present


                        if timestamp not in timestamped_data:

                            timestamped_data[timestamp] = {

                                "instance_id": instance_id,

                                "instance_name": instance_name,

                                "timestamp": timestamp,

                                "date": date_part,  # Add the date field

                                "time": time_part,  # Add the time field

                                "granularity": granularity,

                                "domain_name": domain_name,

                                "project_name": project_name,

                            }

                        # Add the metric value to the timestamped record

                        timestamped_data[timestamp][f"{metric_name}_avg"] = value

                        timestamped_data[timestamp][f"{metric_name}_max"] = max_value

                        timestamped_data[timestamp][f"{metric_name}_min"] = min_value


                        # Update the latest timestamp

                        if latest_timestamp is None or timestamp > latest_timestamp:
                            latest_timestamp = timestamp

                else:

                    print(f"Failed to fetch data for metric {metric_name} of instance {instance_id}")


        # Add the collected timestamped data to the output

        output_data.extend(timestamped_data.values())


    # Convert output to JSON format and print

    output_json = json.dumps(output_data, indent=4)

    with open(f"{script_dir}/output.json", "w") as f:

        json.dump(output_data, f, indent=4)


    # Save the latest timestamp to the file

    if latest_timestamp:

        with open(last_sync_file, "w") as file:

            file.write(latest_timestamp)

        print(f"Updated last sync timestamp to: {latest_timestamp}")



# Function to get a new access token

def get_zoho_token():

    token_url = zoho_token_url

    payload = {

    "grant_type": "refresh_token",

    "refresh_token": zoho_refresh_token,

    "client_id": client_id,

    "client_secret": client_secret,

    "redirect_uri": redirect_uri

}

    response = requests.post(token_url, params=payload)

    if response.status_code == 200:

        return response.json().get('access_token')

    else:

        print("Failed to fetch access token:", response.json())

        return None


def import_to_zoho():
    # Specify the JSON file to send
    json_file = f"{script_dir}/output.json"  
    try:
        # Read the JSON file
        with open(json_file, "rb") as file:
            json_data = json.load(file)   
        # Send the JSON data to the Flask API
        response = requests.post(zoho_api_url, json=json_data)
    
        # Print the response from the server
        print("Response Status Code:", response.status_code)
        print("Response JSON:", response.json())

    except Exception as e:
        print("Error:", e)




def main():
    retrieve_data()
    import_to_zoho()


if __name__ == '__main__':
    main()