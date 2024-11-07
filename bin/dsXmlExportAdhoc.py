import yaml, os, subprocess, shutil, re
from colorama import Fore, Style, init
from tqdm import tqdm
 
# Initialize colorama on Windows
init()
 
# Code by: Gitesh Kolte
# Description: The script will export the XML for the jobs in a given project provided in the input file.

# Read YAML data from configurations.yaml
with open(r'..\config\configurations.yml', 'r') as file:
    yaml_data = yaml.safe_load(file)
# Access other keys in 'datastageLegacy'
datastageLegacy = yaml_data.get('datastageLegacy', {})

# Get the required data from the configurations file
classicPath = datastageLegacy['classicPath']
isDomain = datastageLegacy['isDomain']
host = datastageLegacy['host']
isUser = datastageLegacy['isUser']
isPwd = datastageLegacy['isPwd']
isProject = datastageLegacy['isProject']
exportPath = datastageLegacy['exportPath']
# Input file path
exportListAdhoc = datastageLegacy.get('exportListAdhoc')
# Status File Path
statusPath = datastageLegacy.get('statusPath')
statusFileAdhoc = datastageLegacy.get('statusFileAdhoc')
statusFilePath = os.path.join(statusPath, f'{isProject}{statusFileAdhoc}.csv')

def check_and_create_folder(folder_path):
    # Create status file path if doesnot exists
    os.makedirs(folder_path, exist_ok=True)

def ds_job_export(xml_job_name, xmlExportPath, incDep):
    success_status = None
    
    # Check and create necessary folders
    check_and_create_folder(xmlExportPath)
    tmp_folder = f"{xmlExportPath}\\temp_xml_{xml_job_name}"
    check_and_create_folder(tmp_folder)
    check_and_create_folder(os.path.join('..', 'logs', "xml"))

    # Build the full path to the exported XML file
    xml_file_path = os.path.join(tmp_folder, f"{xml_job_name}.xml")
 
    if incDep == 'N':
        # Construct the command
        job_export_command = [
            f"{classicPath}\\dsexport",
            f"/D={isDomain}",
            f"/H={host}",
            f"/U={isUser}",
            f"/P={isPwd}",
            f"/JOB={xml_job_name}",
            "/XML",
            "/NODEPENDENTS",
            f"{isProject}",
            xml_file_path
        ]

    if incDep == 'Y':
        # Construct the command
        job_export_command = [
            f"{classicPath}\\dsexport",
            f"/D={isDomain}",
            f"/H={host}",
            f"/U={isUser}",
            f"/P={isPwd}",
            f"/JOB={xml_job_name}",
            "/XML",
            f"{isProject}",
            xml_file_path
        ]

    # Run the command
    try:
        subprocess.run(job_export_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        counter = 1
        xml_file_path = os.path.join(tmp_folder, f"{xml_job_name}.xml")
 
        while counter == 1:
            if os.path.exists(xml_file_path):
                xml_file = os.path.getsize(xml_file_path)
                file_size_in_kb = xml_file / 1024
 
                if file_size_in_kb <= 1:
                    log_file = next((file for file in os.listdir(tmp_folder) if file.startswith(f"{xml_job_name}") and file.endswith(".log")), None)
 
                    if log_file:
                        log_file_size_kb = round(os.path.getsize(os.path.join(tmp_folder, log_file)) / 1024, 2)
 
                        if log_file_size_kb > 0:
                            os.rename(os.path.join(tmp_folder, log_file), os.path.join('..', 'logs', "xml", log_file))
                            os.rename(xml_file_path, os.path.join(xmlExportPath, f"{xml_job_name}.xml"))
                            #print(f"{Fore.RED}Export failed for the job {xml_job_name} and the log file {log_file} is moved to {os.path.join('..', 'logs', 'xml')} folder{Style.RESET_ALL}")
                            success_status = False
                            counter = 0
                        else:
                            continue
                    else:
                        continue
                else:
                    attempt = 1
 
                    while attempt == 1:
                        try:
                            # Moving xml from exportXmlTempPath to xmlExportPath
                            new_file_path = os.path.join(xmlExportPath, f"{xml_job_name}.xml")
                            # Move the file
                            shutil.move(xml_file_path, new_file_path)
                            #print(f"{Fore.GREEN}Export successful for the asset {xml_job_name}{Style.RESET_ALL}")
                            success_status = True
                            counter = 0
                            break
                        except Exception:
                            # If an exception is caught, the xml is being created
                            continue
            else:
                continue
 
        # Remove the temporary folder
        shutil.rmtree(tmp_folder)
        # Return boolean value for successful generation of XML files or not
        return success_status

    except subprocess.CalledProcessError as e:
        #print(f"{Fore.RED}Error exporting DataStage job: {e}{Style.RESET_ALL}")
        success_status = False

# Create status file path if doesnot exists
check_and_create_folder(statusPath)

# Get the job details, export it and store in ISX directory
if os.path.isfile(exportListAdhoc):
    with open(f"{exportListAdhoc}") as srcTxtFile:
        with open(statusFilePath, 'w') as status:
            status.write('MasterAsset, Status\n')
        assetLst = [line.strip() for line in srcTxtFile.readlines() if line.strip()]
        if assetLst:
            # Export XML path
            xmlExportPath = os.path.join(exportPath, 'xml')
            # Create ISX Export file path if doesnot exists
            check_and_create_folder(xmlExportPath)
            with tqdm(total=len(assetLst), bar_format='{desc} {percentage:3.0f}%|{bar:60}') as pbar:
                for idx, assetAndIncDep in enumerate(assetLst, start=1):
                    assetName, incDep = assetAndIncDep.split('|')
                    assetName, incDep = assetName.strip(), incDep.strip().upper()
                    # Setting tqdm description
                    pbar.set_description(f"XML Export initializing for {Fore.YELLOW}{assetName}{Style.RESET_ALL} [{idx}/{len(assetLst)}]")
                    # Call subprocess
                    success_status = ds_job_export(assetName, xmlExportPath, incDep)
                    # Write status to status file
                    if (isinstance(success_status, bool)):
                        if success_status == True:
                            with open(statusFilePath, 'a') as status:
                                status.write(f'{assetName}, Success\n')
                        else:
                            with open(statusFilePath, 'a') as status:
                                status.write(f'{assetName}, Failed\n')
                            # Delete the  XML file which was created but failed to be added to status file
                            pattern = rf".*{assetName}.*"
                            # Remove the files which falls under error
                            for root, dirs, files in os.walk(xmlExportPath):
                                for file in filter(lambda x: re.match(pattern, x), files):
                                    os.remove(os.path.join(root, file))
                    pbar.update()
            print(f"\nExport process completed. Please refer to the path {Fore.GREEN}{xmlExportPath}{Style.RESET_ALL} to get the XML/s\nStatus File Path: {Fore.GREEN}{statusFilePath}{Style.RESET_ALL}")
else:           
    print(f"Error opening the file: {Fore.RED}{exportListAdhoc}{Style.RESET_ALL}")
