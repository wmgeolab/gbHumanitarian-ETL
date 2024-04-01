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


@flow(name='HumETLFour',flow_run_name="{Country_iso}",log_prints=True)
#Method to create all Adm level folders for each country
def create_folders(Country_iso):
    # replace this with the ID of the dataset you want to retrieve metadata for
    dataset_id = f"cod-ab-{Country_iso}"
    print(dataset_id)

    #reading particular country dataset from hdx and downloading the zip file
    dataset = Dataset.read_from_hdx(dataset_id)
    resources = dataset.get_resources()
    for res in resources:
        format_value = res['format']
        name = res.get('name', '')
        adm_levels = ["adm0", "adm1", "adm2", "adm3", "adm4","adm5"]
        if format_value == "SHP" and any(adm_level in name for adm_level in adm_levels):
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

    meta_path = os.path.join(extrct_path,"meta.txt")
    with open(meta_path, "w") as f:
        f.write(text)


    #creating admin level directories sames as source directories
    for adm_level in range(6):
        adm_string = f"adm{adm_level}"
        ext = ["cpg","dbf","prj","shp","shx"]
        spt_path = os.path.join(path,  f"{filename}_SPT")
        adm_zip_path = os.path.join(spt_path, f"{filename}_{adm_string.upper()}")
        for file in os.listdir(extrct_path):
            if adm_string in file and file[-3:].lower() in ext:
                os.makedirs(adm_zip_path, exist_ok=True)
                copyfile=os.path.join(extrct_path, file)
                shutil.copy2( copyfile, adm_zip_path)
                print(f"Copied {file} to {adm_zip_path}")
            if file[-3:].lower() == "txt" and os.path.isdir(adm_zip_path):
                copyfile=os.path.join(extrct_path, file)
                with open(copyfile, "r") as f:
                    contents = f.read()
                    # Modify the contents of the text file as required
                    modified_contents = contents.replace("Boundary Type: \n", f"Boundary Type: {adm_string.upper()}\n")
                with open(os.path.join(adm_zip_path, file), "w") as f:
                    f.write(modified_contents)
                print(f"Copied {file} to {adm_zip_path} and added text")

    #zipping the adminlevel directories
    for directory in os.listdir(spt_path):
        inpath=os.path.join(spt_path, directory)
        outpath=os.path.join(source_dir, f"{directory}.zip")
        zip_directory(inpath,outpath)


#ISO Codes of countries
Country_iso=["caf","tcd","civ"]

for country in Country_iso:
    name=country
    create_folders(Country_iso=country)