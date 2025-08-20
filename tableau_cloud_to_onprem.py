import tableauserverclient as TSC
import pantab as pt
import json
import os
import shutil
import zipfile
import logging
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from retrying import retry
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, Inserter, TableName, CreateMode

# Configure logging to write to a file
logging.basicConfig(
    filename='tableau_cloud_to_on_prem.log',
    filemode='a',  # Append to the file; use 'w' to overwrite
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load configuration from JSON file
with open('C:/path_to/config.json') as config_file:
    cf = json.load(config_file)

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def sign_in(server_url, token_name, token_secret, site_id):
    """
    Signs into the specified Tableau server using a personal access token. Retries up to three times in case of failure.

    Args:
        server_url (str): The URL of the Tableau server.
        token_name (str): The name of the personal access token.
        token_secret (str): The secret value of the personal access token.
        site_id (str): The ID of the site to sign into.

    Returns:
        TSC.Server: The authenticated Tableau server object.

    Raises:
        Exception: If sign-in fails after the specified retry attempts.
    """
    try:
        logging.info(f"Signing into Tableau Server Site: {server_url}")
        server = TSC.Server(server_url, use_server_version=True)
        server.version = "3.9"
        server.add_http_options({'verify': cf['http_options']})
        auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id)
        server.auth.sign_in(auth)
        logging.info(f"Logged into {server_url} Successfully!")
        return server
    except TSC.ServerResponseError as e:
        logging.error(f"Failed to sign into {server_url} due to server response error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during sign-in to {server_url}: {e}")
        raise

def sign_out(server):
    """
    Signs out of the specified Tableau server.

    Args:
        server (TSC.Server): The authenticated Tableau server object.

    Returns:
        None
    """
    try:
        server.auth.sign_out()
        logging.info("Signed out of server.")
    except Exception as e:
        logging.error(f"Failed to sign out of server: {e}")

def download_datasource(server, ds_id, download_path):
    """
    Downloads a single datasource from the Tableau server.

    Args:
        server (TSC.Server): The authenticated Tableau server object.
        ds_id (str): The ID of the datasource to download.
        download_path (str): The directory where the downloaded file will be saved.

    Returns:
        None

    Raises:
        Exception: If the download fails.
    """
    try:
        server.datasources.download(ds_id, filepath=download_path, include_extract=True)
        logging.info(f"Downloaded datasource {ds_id} successfully.")
    except TSC.ServerResponseError as e:
        logging.error(f"Failed to download datasource {ds_id} due to {e}")
    except Exception as e:
        logging.error(f"Unexpected error during download: {e}")

def download_datasources_parallel(server, project_name, download_path):
    """
    Downloads datasources from the specified project on the Tableau server in parallel using ThreadPoolExecutor.

    Args:
        server (TSC.Server): The authenticated Tableau server object.
        project_name (str): The name of the project from which to download datasources.
        download_path (str): The directory where the downloaded files will be saved.

    Returns:
        dict: A dictionary of datasource names and their IDs.

    Raises:
        Exception: If any datasource download fails.
    """
    try:
        # Get all project items and their IDs
        all_project_items, pagination_item = server.projects.get()
        project_ids = {p.name: p.id for p in all_project_items}
        project_id = project_ids[project_name]

        # Get all datasource items and filter by project ID
        req_options = TSC.RequestOptions(pagesize=200)
        all_datasource_items, pagination_item = server.datasources.get(req_options)
        datasources = {ds.name: ds.id for ds in all_datasource_items if ds.project_id == project_id}

        # Filter datasources based on the list provided in the configuration
        datasources_to_download = {name: ds_id for name, ds_id in datasources.items() if name in cf['Datasources']}

        # Download datasources in parallel
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(download_datasource, server, ds_id, os.path.join(download_path, name))
                       for name, ds_id in datasources_to_download.items()]

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error while downloading datasource: {e}")

        logging.info(f"Downloaded datasources: {list(datasources_to_download.keys())}")
        return datasources_to_download
    except Exception as e:
        logging.error(f"Failed to download datasources from project {project_name}: {e}")
        raise

def convert_and_extract_hyper_files(download_path):
    """
    Converts .tdsx files to .zip, extracts .hyper files from the .zip archives, 
    and removes .zip and .log files from the download path.

    Args:
        download_path (str): The directory where the .tdsx and .hyper files are located.

    Returns:
        None

    Raises:
        Exception: If any file operation fails.
    """
    try:
        logging.info('Checking for .tdsx files to rename...')

        for f in os.listdir(download_path): 

            # Rename .tdsx files to .zip
            if f.endswith('.tdsx'):


                full_path = os.path.join(download_path, f)              # Full path to the file
                new_name = f[:-5] + '.zip'                              # New name for the file
                new_full_path = os.path.join(download_path, new_name)   # Full path to the new file
                os.rename(full_path, new_full_path)                     # Rename the file
                logging.info(f'Renamed {f} to {new_name}')              # Log the renaming

        logging.info('Extracting .hyper files from .zip archives')

        # Loop through the files in the download path and extract .hyper files from .zip archives
        for z in os.listdir(download_path):
            
            # Extract .hyper files from .zip archives`
            if z.endswith('zip'):   # If the file is a .zip archive containing a .hyper file

                zip_path = os.path.join(download_path, z)           # Full path to the .zip archive

                with zipfile.ZipFile(zip_path, 'r') as zip_object:  # Open the .zip archive

                    # Loop through the files in the .zip archive
                    for file_name in zip_object.namelist():  
 
                        # If the file is a .hyper file
                        if file_name.endswith('.hyper'):       
                                 
                            hyper_file_path = os.path.join(download_path, f'{z[:-4]}.hyper')    # Full path to the .hyper file
                            logging.info(f'Extracting {file_name} to {hyper_file_path}')        # Log the extraction

                            # Extract the .hyper file from the .zip archive
                            with zip_object.open(file_name) as zf, open(hyper_file_path, 'wb') as f:    # Open the .hyper file
                                shutil.copyfileobj(zf, f)   # Copy the .hyper file to the download path

        # Remove .zip and .log files from the directory
        dirlist = os.listdir(download_path)
        for nm in dirlist:
            if '.zip' in nm or '.log' in nm:
                os.remove(os.path.join(download_path, nm))
        logging.info('Conversion and extraction complete.')
    except Exception as e:
        logging.error(f"Failed to convert and extract hyper files in {download_path}: {e}")
        raise

def is_fact_table(table_definition):
    """
    Determine if a table is a fact table based on its schema.

    Args:
        table_definition (TableDefinition): The table definition.

    Returns:
        bool: True if the table is a fact table, False otherwise.
    """
    # Example logic: consider a table as a fact table if it contains numeric columns
    for column in table_definition.columns:
        if column.type in [SqlType.int(), SqlType.big_int(), SqlType.double(), SqlType.numeric(10, 2)]:
            return True
    return False

def check_fact_table_count(hyper_file_path):
    """
    Check the number of fact tables in a Hyper file.

    Args:
        hyper_file_path (str): The path to the Hyper file.

    Returns:
        bool: True if the Hyper file contains exactly one fact table, False otherwise.
    """
    try:
        # Start a new Hyper process
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            # Connect to the Hyper file
            with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
                # Get the list of tables in the Hyper file
                tables = connection.catalog.get_table_names("Extract")
                
                # Count the number of fact tables
                fact_table_count = 0
                for table in tables:
                    table_definition = connection.catalog.get_table_definition(table)
                    if is_fact_table(table_definition):
                        fact_table_count += 1
                
                # Check if there is exactly one fact table
                return fact_table_count == 1

    except Exception as e:
        logging.error(f"Error checking fact table count in {hyper_file_path}: {e}")
        return False

def convert_to_single_fact_table(hyper_file_path, output_file_path):
    """
    Convert a Hyper file with multiple fact tables into a single fact table.

    Args:
        hyper_file_path (str): The path to the input Hyper file.
        output_file_path (str): The path to the output Hyper file.

    Returns:
        bool: True if the conversion is successful, False otherwise.
    """
    try:
        logging.info(f"Starting conversion of {hyper_file_path} to {output_file_path}")
        
        # Start a new Hyper process
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            # Connect to the input Hyper file
            with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as input_connection:
                # Get the list of tables in the input Hyper file
                tables = input_connection.catalog.get_table_names("Extract")
                
                # Count the number of fact tables
                fact_tables = [table for table in tables if is_fact_table(input_connection.catalog.get_table_definition(table))]
                
                # If there is only one fact table, no conversion is needed
                if len(fact_tables) <= 1:
                    logging.info(f"No conversion needed for {hyper_file_path}, only one fact table found.")
                    return True
                
                # Retrieve the schema from the first fact table
                new_fact_table_columns = input_connection.catalog.get_table_definition(fact_tables[0]).columns
                
                # Define the schema for the new fact table
                new_fact_table = TableDefinition(
                    table_name=TableName("Extract", "NewFactTable"),
                    columns=new_fact_table_columns
                )
                
                # Create the new Hyper file and the new fact table
                with Connection(endpoint=hyper.endpoint, database=output_file_path, create_mode=CreateMode.CREATE_AND_REPLACE) as output_connection:
                    output_connection.catalog.create_table(new_fact_table)
                    
                    # Insert data from existing fact tables into the new fact table
                    for table in fact_tables:
                        logging.info(f"Copying data from {table} to NewFactTable")
                        # Copy data from the existing fact table to the new fact table
                        with Inserter(output_connection, new_fact_table) as inserter:
                            inserter.add_rows(input_connection.execute_list_query(f"SELECT * FROM {table}"))
                            inserter.execute()
        
        logging.info(f"Conversion of {hyper_file_path} to {output_file_path} completed successfully.")
        return True

    except Exception as e:
        logging.error(f"Error converting Hyper file {hyper_file_path} to single fact table: {e}")
        return False

def validate_and_convert_hyper_files(download_path):
    """
    Validate and convert Hyper files to ensure they contain exactly one fact table.

    Args:
        download_path (str): The path where the Hyper files are downloaded.

    Returns:
        bool: True if all Hyper files are valid or successfully converted, False otherwise.
    """
    for hyper_file in os.listdir(download_path):
        if hyper_file.endswith('.hyper'):
            hyper_file_path = os.path.join(download_path, hyper_file)
            if not check_fact_table_count(hyper_file_path):
                output_file_path = os.path.join(download_path, f"converted_{hyper_file}")
                if not convert_to_single_fact_table(hyper_file_path, output_file_path):
                    logging.error(f"Failed to convert Hyper file {hyper_file_path}")
                    return False
                # Replace the original file with the converted file
                try:
                    if os.path.exists(output_file_path):
                        os.remove(hyper_file_path)
                        os.rename(output_file_path, hyper_file_path)
                    else:
                        logging.error(f"Converted file {output_file_path} does not exist.")
                        return False
                except FileNotFoundError as e:
                    logging.error(f"Error renaming file {output_file_path} to {hyper_file_path}: {e}")
                    return False
    return True

def publish_datasource(server, ds_item, filepath, publish_mode=TSC.Server.PublishMode.Overwrite):
    """
    Publishes a single .hyper file to the Tableau server.

    Args:
        server (TSC.Server): The authenticated Tableau server object.
        ds_item (TSC.DatasourceItem): The datasource item to publish.
        filepath (str): The path to the .hyper file.
        publish_mode (TSC.Server.PublishMode): The publish mode (default is Overwrite).

    Returns:
        None

    Raises:
        Exception: If the publishing fails.
    """
    try:
        # Replace double backslashes with single backslashes in the file path
        corrected_filepath = filepath.replace('\\\\', '\\')
        
        # Check if the file exists
        if not os.path.isfile(corrected_filepath):
            raise OSError(f"File path does not lead to an existing file: {corrected_filepath}")
        
        # Log the file path for debugging
        logging.debug(f"Publishing file: {corrected_filepath}")

        # Publish the datasource
        server.datasources.publish(ds_item, corrected_filepath, publish_mode)
        logging.info(f'Published {ds_item.name} successfully.')

    except TSC.ServerResponseError as e:
        logging.error(f"Failed to publish {ds_item.name} due to {e}")
    except Exception as e:
        logging.error(f"Unexpected error during publishing: {e}")
        raise

def publish_datasources(server, project_name, download_path):
    """
    Publishes .hyper files to the specified project on the Tableau server in parallel using ThreadPoolExecutor.

    Args:
        server (TSC.Server): The authenticated Tableau server object.
        project_name (str): The name of the project to which to publish datasources.
        download_path (str): The directory where the .hyper files are located.

    Returns:
        None

    Raises:
        Exception: If any publishing operation fails.
    """
    try:
        # Get all project items and their IDs
        all_project_items, pagination_item = server.projects.get()
        project_ids = {p.name: p.id for p in all_project_items}
        project_id = project_ids[project_name]

        # Construct datasource items for each .hyper file
        ds_items = []
        for filename in os.listdir(download_path):
            if filename.endswith('.hyper'):
                print(f"Found file: {filename}")
                # Remove the .hyper extension and ensure only one (fedcrmpro.my.salesforce.com) is added
                ds_name = filename.replace('.hyper', '').replace(' (fedcrmpro.my.salesforce.com)', '')
                ds_item = TSC.DatasourceItem(
                    project_id=project_id, 
                    name=f"{ds_name} (fedcrmpro.my.salesforce.com)"
                )
                ds_items.append(ds_item)

        # Publish datasources in parallel
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(publish_datasource, server, ds_item, os.path.join(download_path, filename), publish_mode=TSC.Server.PublishMode.Overwrite)
                       for ds_item, filename in zip(ds_items, os.listdir(download_path)) if filename.endswith('.hyper')]

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error while publishing datasource: {e}")

        logging.info('Publishing complete.')
    except Exception as e:
        logging.error(f"Failed to publish datasources to project {project_name}: {e}")
        raise

def main():
    """
    Main function to download datasources from Tableau Cloud and publish them to On-Prem Tableau Server.

    Workflow:
        1. Sign in to Tableau Cloud.
        2. Download specified datasources in parallel.
        3. Sign out of Tableau Cloud.
        4. Convert and extract .hyper files.
        5. Validate and convert Hyper files if needed.
        6. Sign in to On-Prem Tableau Server.
        7. Publish .hyper files to the On-Prem server.
        8. Sign out of On-Prem Tableau Server.

    Returns:
        None

    Raises:
        Exception: If any step in the workflow fails.
    """
    try:
        # Sign in to Tableau Cloud
        cloud_server = sign_in(cf['CloudServerURL'], cf['CloudTokenName'], cf['CloudTokenSecret'], cf['CloudSiteID'])
        
        # Download datasources in parallel
        download_datasources_parallel(cloud_server, cf['ProjectName'], cf['DownloadPath'])
        
        # Sign out of Tableau Cloud
        sign_out(cloud_server)
        
        # Convert and extract .hyper files
        convert_and_extract_hyper_files(cf['DownloadPath'])
        
        # Validate and convert Hyper files if needed
        #if not validate_and_convert_hyper_files(cf['DownloadPath']):
            #raise Exception("Failed to validate or convert Hyper files.")
        
        # Sign in to On-Prem Tableau Server
        onprem_server = sign_in(cf['OnPremServerURL'], cf['TokenName'], cf['TokenSecret'], cf['OnPremSiteID'])
        
        # Validate Hyper files before publishing
        for hyper_file in os.listdir(cf['DownloadPath']):

            if hyper_file.endswith('.hyper'):
                hyper_file_path = os.path.join(cf['DownloadPath'], hyper_file)

                #if not check_fact_table_count(hyper_file_path):
                    #raise Exception(f"Hyper file {hyper_file_path} does not contain exactly one fact table.")
        
        # Publish .hyper files to the On-Prem server
        publish_datasources(onprem_server, cf['ProjectName'], cf['DownloadPath'])
        
        # Sign out of On-Prem Tableau Server
        sign_out(onprem_server)

    except Exception as e:
        input(f"{e}: ")
        logging.error(f"Failed to complete the main operation: {e}")

if __name__ == "__main__":
    main()
