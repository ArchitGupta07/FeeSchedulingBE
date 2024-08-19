from fastapi import File, HTTPException, Query, UploadFile, Depends
from fastapi import APIRouter
from services.comparision import Comparision
from services.table_manager import TableManager
from fastapi.responses import FileResponse, StreamingResponse

from db import Database, get_db, engine




file_router = APIRouter()
# @file_router.get("/")
# def get_all_uploaded_files(db : Database = Depends(get_db),table_manager_obj: TableManager = Depends(TableManager)) : 
#     response = table_manager_obj.get_all_files(db)
#     return {"data" : response}
# def get_table_manager(db : Database = Depends(get_db)):
#     return TableManager(db)


@file_router.get("/")
def get_all_uploaded_files( statename: str = Query(None),  # Query parameter for state name
    category: str = Query(None), db : Database = Depends(get_db),table_manager_obj: TableManager = Depends(TableManager)) : 
    response = table_manager_obj.get_all_files(db,statename,category)
    return {"data" : response}

@file_router.get('/download/{table_name}')
def download_file(table_name:str,db : Database = Depends(get_db),table_manager_obj: TableManager = Depends(TableManager)) : 
    file_path = table_manager_obj.download_xls(db,table_name)
    return FileResponse(file_path, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename="download.xlsx")

    


@file_router.get("/file-data/{table_name}",   summary="Fetch Data from a Specified Table",
    description="""
    This API endpoint retrieves data from a specified table in the database.
    It takes the table name as a path parameter and returns the data stored in that table.
    
    **Path Parameter:**
    - `table_name` (str): The name of the table from which to fetch the data.
    
    **Response:**
    - JSON object containing the data from the specified table.
    
    Example:
    If the table name is `users`, the API will return the data stored in the `users` table.
    """)
async def get_file_data(table_name : str,db : Database = Depends(get_db),cmp_obj : Comparision = Depends(Comparision)) :
    table_data  = cmp_obj.get_table_data(table_name,db)
    return {"data" : table_data}


@file_router.post("/upload",summary="Upload an XLSX File and Store Data in Database",
    description=(
        "Uploads an XLSX file, extracts data from it, and stores the extracted data "
        "in the database. The file should be in XLSX format, and the data will be inserted "
        "into a database table managed by the TableManager."
    ))
async def upload_file(file: UploadFile = File(...), table_manager_obj: TableManager = Depends(TableManager),db : Database = Depends(get_db)):
    data,table_name,file_name = await table_manager_obj.insert_table(db,file)
    return {"data" : data, "table_name" : table_name, "file_name" : file_name}


@file_router.post("/compare/{table_name}",  summary="Compare XLSX File with Existing Table Data",
    description=(
        "Uploads an XLSX file containing data to be compared with an existing table "
        "in the database. The file should have a similar structure to the data in the "
        "specified table. The endpoint compares the new file with the existing data and "
        "returns the differences or changes found."
    ))
async def calculate_dif(table_name: str ,cmp_file: UploadFile = File(...),cmp_obj : Comparision = Depends(Comparision),db : Database = Depends(get_db)) : 
    #print(table_name)
    response = await cmp_obj.compare(db,table_name,cmp_file)
    return {"data" : response}


@file_router.put("/update/{table_name}")
async def update_item(table_name:str, item: dict):
    if item:
        print(item)        
        return {"message": "Tabel updated successfully"}
    else:
        raise HTTPException(status_code=404, detail="Unsuccesful table update Attempt")

