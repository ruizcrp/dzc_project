from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import requests
import zipfile
import os
import numpy as np

path_zip="data/zip/"
path_unzipped="data/unzipped/"
path_csv="data/csv/"
path_parquet="data/parquet/"


@task(retries=10,log_prints=True)
def download_zip(dataset_url: str,dataset_file: str) -> None:
    """Download the education data in zip-format"""
    r = requests.get(dataset_url, allow_redirects=True)
    open(f'{path_zip}{dataset_file}.zip', 'wb').write(r.content)

@task(log_prints=True)
def extract_zip(file_name: str) -> None:
    """This will extract the large zip-file"""
    with zipfile.ZipFile(f"{path_zip}{file_name}.zip", 'r') as zip_ref:
        zip_ref.extractall(f"{path_unzipped}")
    os.remove(f"{path_zip}{file_name}.zip") 

@task(log_prints=True)
def construct_filename() -> str:
    """Became necessary as the files are not always called the same"""
    res = []
    # Iterate directory
    for file in os.listdir(path_unzipped):
        # check only text files
        if file.endswith('.accdb'):
            res.append(file)
    if len(res)!=1:
        print("Warning: Should not happen")
    print(res)
    return res[0]

@task(log_prints=True)
def extract_accessdb(file_name:str, table_name: str)-> None:
    """We will need to extract the tables from the accessdb-file into csv-files first"""
    file_name=file_name.replace(" ","\ ")
    temp_path=Path(f"{path_unzipped}{file_name}")

    print(f"Extracting file_name:{temp_path}")
    os.system(f"mdb-export {temp_path} 'Annual EM {table_name}' > {path_csv}{table_name}.csv")

@task(log_prints=True)
def process_table(table_name:str)-> pd.DataFrame:
    """Now the actual processing of the tables happens"""
    #load in the respective file
    df=pd.read_csv(f"{path_csv}{table_name}.csv",usecols=["ENTITY_NAME","YEAR","ASSESSMENT_NAME","SUBGROUP_NAME","PER_PROF"],dtype={"ENTITY_NAME":str,"YEAR":int,"ASSESSMENT_NAME":str,"SUBGROUP_NAME":str,"PER_PROF":str})
    # select all the counties, and the totals for the charter schools as well as the total for public schools
    df=df[df["ENTITY_NAME"].str.contains("Charter Schools|County|All Public Schools")]
    # only select the rows with "All Students" as there are also rows per gender, per origin, per situation of needs etc.
    df=df[df["SUBGROUP_NAME"]=="All Students"]
    # select the highest levels of each subject, which is level 8
    list_of_highest_levels=["ELA8","MATH8","Science8"]
    df=df[df["ASSESSMENT_NAME"].isin(list_of_highest_levels)]
    # As there are data from semester 2 of old year and semester 1 of new year, we will create a better year_semester variable reflecting that
    max_year=df["YEAR"].max()
    df['YEAR_SEMESTER'] = df['YEAR'].apply(lambda x: str(x)+"S1" if x==max_year else str(x)+"S2")
    df = df.drop('YEAR', axis=1)
    print(df.head())
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, file_name: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{path_parquet}{file_name}.parquet")
    df.to_parquet(path)
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcsbucketblock")
    print("gcsbucketblock loaded")
    print(f"Path is:{Path(path)}")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year) -> None:
    """The main ETL function"""
    print(f"Starting etl_web_to_gcs function with year:{year}")

    #first of all create the folders if they do not exist yet
    Path(path_zip).mkdir(parents=True, exist_ok=True)
    Path(path_unzipped).mkdir(parents=True, exist_ok=True)
    Path(path_csv).mkdir(parents=True, exist_ok=True)
    Path(path_parquet).mkdir(parents=True, exist_ok=True)
    
    year_short=str(year)[2:]
    print(f"year_short is: {year_short}")
    year_minus=str(int(year_short)-1)
    file_name = f"SRC20{year_short}"
    dataset_url = f"https://data.nysed.gov/files/essa/{year_minus}-{year_short}/{file_name}.zip"

    download_zip(dataset_url,file_name)
    extract_zip(file_name)
    list_of_subjects=["ELA","Math","Science"]
    appended_df= []
    access_file_name=construct_filename()
    print(access_file_name)
    for subject in list_of_subjects:
        extract_accessdb(access_file_name, subject)
        df=process_table(subject)
        appended_df.append(df)
    appended_df = pd.concat(appended_df)

    path = write_local(appended_df, file_name)
    write_gcs(path)
    # clean up
    os.system(f'rm -rf {path_zip}')
    os.system(f'rm -rf {path_unzipped}')
    os.system(f'rm -rf {path_csv}')
    os.system(f'rm -rf {path_parquet}')


@flow()
def etl_parent_flow(
  years: list[int] = [2022,2021,2019,2018]
):
    for year in years:
        etl_web_to_gcs(year)

if __name__ == "__main__":
    years = [2022,2021,2019,2018]
    etl_parent_flow(years)
