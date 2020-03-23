import os
from .database import dboperations


class ScanDirectories:
    dbconnection = None

    def __init__(self, path, dbname):
        self.dbconnection = dboperations.DbOperations(path, dbname)

    def scan_dirs(self, drive, table_name, column_name):
        exclude_list = ['$RECYCLE.BIN', 'System Volume Information', '$Recycle.Bin', '$SysReset', 'violentpython']
        for f in os.scandir(drive):
            if f.is_dir():
                if f.name not in exclude_list:
                    self.scan_dirs(f.path, table_name, column_name)
            if f.is_file():
                # if os.path.splitext(f.name)[1].lower() in ext:
                dir_name = os.path.dirname(f.path)
                sub_folder_name = os.path.split(dir_name)[-1]
                dir_name = dir_name[0:dir_name.rfind("\\")]
                drive_name = os.path.splitdrive(f.path)[0]
                file_name = f.name
                file_size = os.path.getsize(f.path)
                file_size = round((file_size / 1024), 2)
                record = [drive_name, dir_name, sub_folder_name, file_name, file_size]
                self.dbconnection.insert_data(table_name, record, column_name)
