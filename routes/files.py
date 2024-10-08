from typing import List
from fastapi import File, HTTPException, Query, UploadFile, Depends, Form
from fastapi import APIRouter
from pydantic import BaseModel
from services.comparision import Comparision
from services.version_maintainer import VersionManager
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
def get_all_uploaded_files( statename: str = Query(None), 
    category: str = Query(None), db : Database = Depends(get_db),table_manager_obj: TableManager = Depends(TableManager)) : 
    response = table_manager_obj.get_all_files(db,statename,category)
    return {"data" : response}

@file_router.get('/download/{table_name}')
def download_file(table_name:str,db : Database = Depends(get_db),table_manager_obj: TableManager = Depends(TableManager)) : 
    file_path = table_manager_obj.download_xls(db,table_name)
    return FileResponse(file_path, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename="download.xlsx")

    


@file_router.get("/file-data/{table_name}/{id}",   summary="Fetch Data from a Specified Table",
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
async def get_file_data(table_name : str,id:str,db : Database = Depends(get_db),cmp_obj : Comparision = Depends(Comparision)):
    # print("id...............",id)
    table_data, active_columns  = cmp_obj.get_table_data(table_name,id, db)
    return {"data" : table_data,"active_columns":active_columns}


@file_router.post("/upload",summary="Upload an XLSX File and Store Data in Database",
    description=(
        "Uploads an XLSX file, extracts data from it, and stores the extracted data "
        "in the database. The file should be in XLSX format, and the data will be inserted "
        "into a database table managed by the TableManager."
    ))
async def upload_file(file: UploadFile = File(...), stateName: str = Form(...),category: str = Form(...),table_manager_obj: TableManager = Depends(TableManager),db : Database = Depends(get_db)):

    print("upload",stateName, category)
    table_name,file_name, version_id = await table_manager_obj.insert_table(db,stateName, category,file)
    return {"table_name" : table_name, "file_name" : file_name, "version_id":version_id}


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


@file_router.get("/get_cell_changes/{id}")
async def get_file_data(id : str,db : Database = Depends(get_db),cmp_obj : Comparision = Depends(Comparision)) :
    cell_changes  = cmp_obj.get_cell_changes(id,db)
    table_changes = cmp_obj.get_table_changes(id,db)
    return {"cell_changes" : cell_changes, "table_changes":table_changes}

# @file_router.put("/update/{table_name}")
# async def update_item(table_name:str, item: dict):
#     if item:
#         print(item)        
#         return {"message": "Tabel updated successfully"}
#     else:
#         raise HTTPException(status_code=404, detail="Unsuccesful table update Attempt")





class FileChanges(BaseModel):
    newColumns: List[str]
    deletedCols: List[str]

@file_router.post("/update/{tableName}")
async def update_file(tableName: str, changes: FileChanges, version_obj:VersionManager=Depends(VersionManager), db : Database = Depends(get_db)):
    try:
        # Process the file changes here
        new_columns = changes.newColumns
        deleted_cols = changes.deletedCols
        
        # Here you can add your logic to update the database or file system
        # For demonstration purposes, we'll just return a success message.
        
        # Replace the following with your actual update logic
        print(f"Table Name: {tableName}")
        print(f"New Columns: {new_columns}")
        print(f"Deleted Columns: {deleted_cols}")

        version_obj.apply_new_changes(tableName)
        
        # Returning a success response
        return {"message": "File updated successfully"}
    
    except Exception as e:
        # Handle any exceptions that may occur
        raise HTTPException(status_code=500, detail=str(e))