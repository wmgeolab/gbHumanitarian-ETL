import os
import hashlib
import time
import json
import zipfile
import datetime
import random
from datetime import date
from prefect import flow
from decouple import config
from github import Github
from urllib.request import urlopen
from urllib.parse import quote_plus

downData = "/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/SourceData"
gitData = "/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/gitData/geoBoundaries/sourceData/gbHumanitarian"

# @flow(log_prints=True)
# def hi(dir1, dir2):

@flow(name='Check and Pull',flow_run_name="{branchname}",log_prints=True)
def submit_to_github(branchname, title, body, src, dst, basename_hash):
    # init
    g = Github(config('GITHUB_TOKEN', default='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'))
    upstream = g.get_repo('rohith4444/geoBoundaries') # upstream
    upstream_branch = 'main'
    # get or create the fork
    try:
        # get existing fork
        fork = g.get_user().get_repo('geoBoundaries')
    except:
        # fork doesn't already exist, eg if the geoBoundaryBot's fork has been deleted/cleaned up
        fork = g.get_user().create_fork(upstream)

    # check if pull request already exists
    existing_pulls = upstream.get_pulls(base=upstream_branch)
    print("Crossed it")

    if existing_pulls.totalCount == 0:

        # create new branch based on upstream
        fork.create_git_ref(ref='refs/heads/' + branchname, 
                            sha=upstream.get_git_ref(ref='heads/' + upstream_branch).object.sha)
        # check if file already exists in the repository
        try:
            file = fork.get_contents(dst, upstream_branch)
            existing_sha = file.sha
        except:
            existing_sha = None
        # create commit message and content for the file
        commit_message = 'Updated {}'.format(dst)
        content = open(src, mode='rb').read()
        # create or update the file in the repository
        if existing_sha is None:
            fork.create_file(dst, commit_message, content, branch=branchname)
        else:
            # get sha of existing file by inspecting parent folder's git tree
            # get_contents() is easier but downloads the entire file and fails
            # for larger filesizes
            dest_folder = os.path.dirname(dst)
            tree_url = 'https://api.github.com/repos/{}/{}/git/trees/{}:{}'.format(fork.owner.login, fork.name, branchname, quote_plus(dest_folder))
            tree = json.loads(urlopen(tree_url).read())
            # loop files in tree until file is found
            for member in tree['tree']:
                if dst.endswith(member['path']):
                    existing_sha = member['sha']
                    break
            fork.update_file(dst, commit_message, content, sha=existing_sha, branch=branchname)

            #make pull request
            pull = upstream.create_pull(title, body, base=upstream_branch, head=branchname)
            
            # return the url
            pull_url = 'https://github.com/rohith4444/geoBoundaries/pull/{}'.format(pull.number)
            print(pull_url)
            time.sleep(70)

    else:
        for pull in existing_pulls:
            if basename_hash in pull.body:
                print('Pull request already exists.')
                return

        # create new branch based on upstream
        print("entered into else")
        fork.create_git_ref(ref='refs/heads/' + branchname, 
                            sha=upstream.get_git_ref(ref='heads/' + upstream_branch).object.sha)
        # check if file already exists in the repository
        try:
            file = fork.get_contents(dst, upstream_branch)
            existing_sha = file.sha
        except:
            existing_sha = None
        # create commit message and content for the file
        commit_message = 'Updated {}'.format(dst)
        content = open(src, mode='rb').read()
        # create or update the file in the repository
        if existing_sha is None:
            fork.create_file(dst, commit_message, content, branch=branchname)
        else:
            # get sha of existing file by inspecting parent folder's git tree
            # get_contents() is easier but downloads the entire file and fails
            # for larger filesizes
            dest_folder = os.path.dirname(dst)
            tree_url = 'https://api.github.com/repos/{}/{}/git/trees/{}:{}'.format(fork.owner.login, fork.name, branchname, quote_plus(dest_folder))
            tree = json.loads(urlopen(tree_url).read())
            # loop files in tree until file is found
            for member in tree['tree']:
                if dst.endswith(member['path']):
                    existing_sha = member['sha']
                    break
            fork.update_file(dst, commit_message, content, sha=existing_sha, branch=branchname)

            #make pull request
            pull = upstream.create_pull(title, body, base=upstream_branch, head=branchname)
            
            # return the url
            pull_url = 'https://github.com/rohith4444/geoBoundaries/pull/{}'.format(pull.number)
            print(pull_url)
            time.sleep(70)


def generate_md5(file_name):
    md5_hash = hashlib.md5(file_name.encode())
    return md5_hash.hexdigest()

def compare_files(file1_path, file2_path,basename):
    if os.path.isfile(file1_path) and os.path.isfile(file2_path):
        file1_extension = os.path.splitext(file1_path)[1]
        file2_extension = os.path.splitext(file2_path)[1]
        if file1_extension and file2_extension in [".txt", ".csv", ".xml", ".json"]:
            with open(file1_path, "r", encoding="utf-8") as f1, open(file2_path, "r", encoding="utf-8") as f2:
                hash1 = hashlib.sha256(f1.read().encode()).hexdigest()
                hash2 = hashlib.sha256(f2.read().encode()).hexdigest()
        else:
            with open(file1_path, "rb") as f1, open(file2_path, "rb") as f2:
                hash1 = hashlib.sha256(f1.read()).hexdigest()
                hash2 = hashlib.sha256(f2.read()).hexdigest()

        if hash1 == hash2:
            print(f"{file1_path} and {file2_path} are the same")
        else:
            print(f"{file1_path} and {file2_path} are different")
            src=f"/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/SourceData/{basename}"
            now = datetime.datetime.now()
            month = now.strftime("%B")  # Month name, full version
            today = date.today()
            todaydate = today.strftime("%d")
            year = today.strftime("%Y")
            random_number = random.random()
            dst = f"sourceData/gbHumanitarian/{basename}"
            filename=os.path.splitext(basename)[0]
            branchname=f"pull_{filename}_{year}_{month}{todaydate}_{random_number}"
            base=basename.split(".")[0]
            title=f"{base} UNOCHA Automated Retrieval {month} {todaydate} {year}"
            basename_hash = generate_md5(basename)
            body=f"""Pull request to update {basename} and the hash value {basename_hash}.
                     This automated pull request has been generated to update the existing files with the most recent versions obtained from UNOCHA."""
            submit_to_github(branchname,title,body,src,dst,basename_hash)            


def compare_directories(dir1, dir2):
    dir1_files = set(os.listdir(dir1))
    dir2_files = set(os.listdir(dir2))
    common_dirs = dir1_files.intersection(dir2_files)

    for file in common_dirs:
        file1_path = os.path.join(dir1, file)
        basename = os.path.basename(file1_path)
        print(basename)
        file2_path = os.path.join(dir2, file)
        file1_extension = os.path.splitext(file1_path)[1]
        file2_extension = os.path.splitext(file2_path)[1]
        print(file1_path)
        print(file1_extension)
        print(file2_path)
        print(file2_extension)
        # Check if the file is a zip file
        if file1_extension and file2_extension == ".zip":
            with zipfile.ZipFile(file1_path, 'r') as zip1, zipfile.ZipFile(file2_path, 'r') as zip2:
                zip1_files = set(zip1.namelist())
                zip2_files = set(zip2.namelist())

                for file1_path in zip1_files:
                    file1_extension = os.path.splitext(file1_path)[1]
                    matching_files = [file2_path for file2_path in zip2_files if os.path.splitext(file2_path)[1] == file1_extension]
                    if len(matching_files) > 0:
                        # Only proceed if there is at least one matching file with the same extension
                        file2_path = matching_files[0]  # Pick the first matching file
                        zip_file1_path = os.path.join(dir1, file1_path)
                        zip_file2_path = os.path.join(dir2, file2_path)

                        # Extract the file from the zip
                        zip1.extract(file1_path, path=os.path.join(dir1))
                        zip2.extract(file2_path, path=os.path.join(dir2))

                        compare_files(zip_file1_path, zip_file2_path, basename)

                        # Remove the extracted files
                        os.remove(zip_file1_path)
                        os.remove(zip_file2_path)
        else:
            compare_files(file1_path, file2_path, basename)


compare_directories(downData,gitData)

# if __name__ == "__main__":
#     hi(downData,gitData)


