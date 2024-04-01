from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from prefect import flow
import os
import requests
import shutil
import zipfile
import re

#path to save the files
download_dir = "/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/Data"
source_dir = "/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/SourceData"

#configuring hdx api
Configuration.create(hdx_site="prod", user_agent="First Trial", hdx_read_only=True)

#Method to zip the files
def zip_directory(path, zip_path):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, arcname=file)
    print(f"Successfully zipped {path} to {zip_path}")


@flow(name='HumETLThree',flow_run_name="{Country_iso}",log_prints=True)
#Method to create all Adm level folders for each country
def create_folders(Country_iso):

    #reading particular country dataset from hdx and downloading the zip file
    dataset = Dataset.read_from_hdx(Country_iso)
    resources = dataset.get_resources()
    for res in resources:
        format_value = res['format']
        if format_value == "SHP":
            url = res['url']
            print(url)
            filenamezip = os.path.basename(url)
            filename= filenamezip[:3]
            filename = filename.upper()
            path = os.path.join(download_dir, filename)
            os.makedirs(path, exist_ok=True)
            response = requests.get(url)
            filepath = os.path.join(path, filenamezip)
            with open(filepath, 'wb') as f:
                f.write(response.content)
            break
    print("Resource URL %s downloaded to %s" % (url, path))

    #extracting the zip file
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        extrct_path = os.path.join(path,f"{filename}_EXT")
        zip_ref.extractall(extrct_path)

    #creating meta data file 
    # send GET request to retrieve dataset metadata
    url2 = f"https://data.humdata.org/api/3/action/package_show?id={dataset_id}"
    response2 = requests.get(url2)

    # check if the request was successful
    if response2.status_code == 200:
        # extract the metadata from the response JSON
        metadata = response2.json()["result"]
    else:
        # handle the error
        print(f"Failed to retrieve metadata for dataset {dataset_id}: {response.status_code}")


    # Extract the specific metadata fields you want
    source = metadata["dataset_source"]
    cavets = metadata.get("caveats", "")
    # Use regular expressions to replace the whitespace between sentences with a single space
    cavets = re.sub(r'\s*([.?!:\]])\s*', r'\1 ', cavets)
    cavets = cavets.replace("\n\n", " ")
    Updated_cavets = f"Link to Notes:  https://data.humdata.org/dataset/{dataset_id}"
    license = metadata["license_title"]
    Updated_license = "Creative Commons Attribution 3.0 Intergovernmental Organisations (CC BY 3.0 IGO)"
    contributor = metadata["organization"]["title"]
    date = metadata["dataset_date"]
    date = date[1:5]

    text = f"Boundary Representative of Year: {date}\n" \
        f"ISO-3166-1 (Alpha-3): {filename}\n" \
        f"Boundary Type: \n" \
        f"Canonical Boundary Type Name:\n" \
        f"Source 1: {source}\n" \
        f"Source 2: {contributor}\n" \
        f"Release Type: gbHumanitarian \n" \
        f"License: {Updated_license if license == 'Creative Commons Attribution for Intergovernmental Organisations' else license}\n" \
        f"License Notes:\n" \
        f"License Source:  https://data.humdata.org/dataset/{dataset_id}\n" \
        f"Link to Source Data:  https://data.humdata.org/dataset/{dataset_id}\n" \
        f"Other Notes: {Updated_cavets if len(cavets)>95 else cavets}\n"
    
    #Delete the directory SYR2
    subdirectory = os.path.join(extrct_path, "SYR2")
    if os.path.exists(subdirectory) and os.path.isdir(subdirectory):
        shutil.rmtree(subdirectory)


    #creating admin level directories sames as source directories
    for adm_level in range(6):
        adm_string = f"admin{adm_level}"
        upd_adm_string = "adm" + adm_string[-1]
        ext = ["cpg","dbf","prj","shp","shx"]
        spt_path = os.path.join(path,  f"{filename}_SPT")
        adm_zip_path = os.path.join(spt_path, f"{filename}_{adm_string.upper()}")
        if os.path.isdir(extrct_path):
            for subdir in os.listdir(extrct_path):
                subdir_path = os.path.join(extrct_path, subdir)
                if os.path.isdir(subdir_path):
                    # construct the new name for the subdirectory
                    new_subdir_name = f"{filename}2"
                    new_subdir_path = os.path.join(extrct_path, new_subdir_name)
                    
                    # rename the subdirectory
                    os.rename(subdir_path, new_subdir_path)
                    #writing meta file
                    meta_path = os.path.join(new_subdir_path,"meta.txt")
                    with open(meta_path, "w") as f:
                        f.write(text)
                    for file in os.listdir(new_subdir_path):
                        if adm_string in file and file[-3:].lower() in ext:
                            os.makedirs(adm_zip_path, exist_ok=True)
                            copyfile=os.path.join(new_subdir_path, file)
                            shutil.copy2( copyfile, adm_zip_path)
                            print(f"Copied {file} to {adm_zip_path}")
                        if file[-3:].lower() == "txt" and os.path.isdir(adm_zip_path):
                            copyfile=os.path.join(new_subdir_path, file)
                            with open(copyfile, "r") as f:
                                contents = f.read()
                                # Modify the contents of the text file as required
                                modified_contents = contents.replace("Boundary Type: \n", f"Boundary Type: {upd_adm_string.upper()}\n")
                            with open(os.path.join(adm_zip_path, file), "w") as f:
                                f.write(modified_contents)
                            print(f"Copied {file} to {adm_zip_path} and added text")

    #zipping the adminlevel directories
    for directory in os.listdir(spt_path):
        inpath=os.path.join(spt_path, directory)
        outpath=os.path.join(source_dir, f"{directory}.zip")
        zip_directory(inpath,outpath)


#ISO Code for a particular country
dataset_id = "cod-ab-syr"

create_folders(Country_iso=dataset_id)