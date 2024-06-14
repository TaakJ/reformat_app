from exception import CustomException
from setup import Folder
import logging
from pathlib import Path
from os.path import join
import glob
import shutil
import pandas as pd
import openpyxl
from openpyxl.styles import Font
import chardet
from io import StringIO
import re
import xlrd

class convert_2_files:

    async def check_source_files(self) -> None:
        
        logging.info("Check Source files..")
        
        set_log = []
        for source, value in self.config.items():
            if source in self.require_source:
                _dir = value["dir"]
                
                status_file = "not_found"
                if glob.glob(_dir, recursive=True):
                    status_file = "found"
                    
                item =  {
                    "source": source,
                    "dir_input": value["dir"],
                    "status_file": status_file,
                    "dir_output": value["dir_output"],
                    "function": "check_source_files"
                }
                set_log.append(item)
                logging.info(f'Source file: "{_dir}", Status: "{status_file}"')
                
        self._log_setter(set_log) 

    
    async def retrieve_data_from_source_files(self) -> None:

        logging.info("Retrieve Data from Source files..")
        
        state = "failed"
        for key, record in enumerate(self.logging):
            record.update({'function': "retrieve_data_from_source_files", 'state': state})

            _data = []
            _dir = record["dir_input"]
            types = Path(_dir).suffix
            status_file = record["status_file"]
            
            try:
                if status_file == "found":
                    if [".xlsx", ".xls"].__contains__(types):
                        logging.info(f'Read Excel file: "{_dir}"')
                        _data = self.excel_data_cleaning(key)
                        
                    else:
                        logging.info(f'Read Text file: "{_dir}"')
                        _data = self.text_data_cleaning(key)
                else:
                    continue
                
                state = "succeed"
                record.update({"data": _data, "state": state})
                
            except Exception as err:
                record.update({'errors': err})

            if "errors" in record:
                raise CustomException(errors=self.logging)
            
        
    def read_text_files(func):
        def wrapper(*args:tuple, **kwargs:dict) -> dict:  
            
            by_lines = iter(func(*args, **kwargs))
            _data = {}
            
            rows = 0
            while True:
                try:
                    list_by_lines = []
                    for source, data in  next(by_lines).items():
                        
                        if source == "LDS":
                            if rows == 0:
                                ## herder column
                                list_by_lines = " ".join(data).split(' ')
                            else:
                                ## row value
                                for idx, value in enumerate(data):
                                    if idx == 0:
                                        value = re.sub(r'\s+',',', value).split(',')
                                        list_by_lines.extend(value)
                                    else:
                                        list_by_lines.append(value)
                                        
                        elif source == "DOC":
                            if rows == 1:
                                ## herder column
                                list_by_lines = " ".join(data).split(' ')
                            elif rows > 1:
                                ## row value
                                for idx, value in enumerate(data):
                                    if idx == 3:
                                        value = re.sub(r'\s+',',', value).split(',')
                                        list_by_lines.extend(value)
                                    else:
                                        list_by_lines.append(value)
                                        
                        elif source == "ADM":
                            ## row value
                            list_by_lines = data
                        
                        if list_by_lines != []:
                            if source not in _data:
                                _data[source] = [list_by_lines]
                            else:
                                _data[source].append(list_by_lines)
                        else:
                            continue
                    rows += 1
                    
                except StopIteration:
                    break
                
            return _data
        return wrapper
        
    
    @read_text_files
    def text_data_cleaning(self, key:int) -> any:

        # logging.info("Cleansing Data From Text file..")
        self.logging[key].update({"function": "text_data_cleaning"})
        
        _dir = self.logging[key]["dir_input"]
        source = self.logging[key]["source"]
        
        files = open(_dir, "rb")
        encoded = chardet.detect(files.read())["encoding"]
        files.seek(0)
        decode_data = StringIO(files.read().decode(encoded))
        
        for lines in decode_data:
            regex = re.compile(r"\w+.*")
            find_regex = regex.findall(lines)
            if find_regex != []:
                yield {source: re.sub(r"\W\s+","||","".join(find_regex).strip()).split("||")}
    
    
    def read_excle_files(func):
        def wrapper(*args:tuple, **kwargs:dict) -> dict:
            
            by_sheets = iter(func(*args, **kwargs))
            _data = {}
            
            while True:
                try:
                    for sheets, data in next(by_sheets).items():
                        
                        if not all(dup == data[0] for dup in data) and not data.__contains__("Centralized User Management : User List."):
                            if sheets not in _data:
                                _data[sheets] = [data]
                            else:
                                _data[sheets].append(data)
                                
                except StopIteration:
                    break
                
            return _data
        return wrapper
    
    
    @read_excle_files
    def excel_data_cleaning(self, key:int) -> any:

        # logging.info("Cleansing Data From Excle files..")
        self.logging[key].update({"function": "excel_data_cleaning"})
        
        workbook = xlrd.open_workbook(self.logging[key]['dir_input'])
        sheet_list = [sheet for sheet in workbook.sheet_names() if sheet != "StyleSheet"]
        
        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                yield {sheets: [cells.cell(row, col).value for col in range(cells.ncols)]}


    async def write_data_to_tmp_file(self, source) -> None:

        logging.info("Write Data to Tmp files..")
        
        source_name = join(Folder.TEMPLATE, "Application Data Requirements.xlsx")
        tmp_name = join(Folder.TMP, f"{source}_TMP-{self.batch_date.strftime('%Y%m%d')}.xlsx")
        print(tmp_name)
        state = "failed"
        
        for record in self.logging:
            try:
                target = record["target"]
                print(target)
                
            except Exception as err:
                record.update({"errors": err})

        # for key in self.logging:
        #     try:
        #         if key["source"] == "Target_file":
        #             key.update(
        #                 {
        #                     "full_path": tmp_name,
        #                     "function": "write_data_to_tmp_file",
        #                     "status": status,
        #                 }
        #             )
        #             ## get new data.
        #             new_df = pd.DataFrame(key["data"])
        #             new_df["remark"] = "Inserted"
        #             try:
        #                 workbook = openpyxl.load_workbook(tmp_name)
        #                 get_sheet = workbook.get_sheet_names()
        #                 sheet_num = len(get_sheet)
        #                 sheet_name = f"RUN_TIME_{sheet_num - 1}"
        #                 sheet = workbook.get_sheet_by_name(sheet_name)
        #                 workbook.active = sheet_num

        #             except FileNotFoundError:
        #                 ## copy files from template.
        #                 status = self.copy_worksheet(source_name, tmp_name)
        #                 workbook = openpyxl.load_workbook(tmp_name)
        #                 sheet = workbook.worksheets[0]
        #                 sheet_name = "RUN_TIME_1"
        #                 sheet_num = 1
        #                 sheet.title = sheet_name

        #             logging.info(f"Generate Sheet_name: {sheet_name} in Tmp files.")

        #             # read tmp files.
        #             data = sheet.values
        #             columns = next(data)[0:]
        #             tmp_df = pd.DataFrame(data, columns=columns)

        #             if status != "succeed":
        #                 tmp_df = tmp_df.loc[tmp_df["remark"] != "Removed"]
        #                 ## create new sheet.
        #                 sheet_name = f"RUN_TIME_{sheet_num}"
        #                 sheet = workbook.create_sheet(sheet_name)
        #             else:
        #                 tmp_df["remark"] = "Inserted"

        #             new_data = self.validation_data(tmp_df, new_df)
        #             ## write to tmp files.
        #             status = self.write_worksheet(sheet, new_data)
        #             workbook.move_sheet(workbook.active, offset=-sheet_num)
        #             workbook.save(tmp_name)

        #             key.update({"sheet_name": sheet_name, "status": status})
        #             logging.info(f"Write to Tmp files status: {status}.")

        #     except Exception as err:
        #         key.update({"errors": err})

        #     if "errors" in key:
        #         raise CustomException(errors=self.logging)


    def write_worksheet(self, sheet: any, new_data: dict) -> str:

        self.logging[-1].update({"function": "write_worksheet"})
        max_rows = max(new_data, default=0)
        logging.info(f"Data for write: {max_rows}. rows")
        start_rows = 2

        try:
            # write columns.
            for idx, columns in enumerate(new_data[start_rows].keys(), 1):
                sheet.cell(row=1, column=idx).value = columns
            ## write data.
            while start_rows <= max_rows:
                for remark in [
                    new_data[start_rows][columns]
                    for columns in new_data[start_rows].keys()
                    if columns == "remark"
                ]:
                    for idx, values in enumerate(new_data[start_rows].values(), 1):
                        if start_rows in self.skip_rows and remark == "Removed":
                            sheet.cell(row=start_rows, column=idx).value = values
                            sheet.cell(row=start_rows, column=idx).font = Font(
                                bold=True, strike=True, color="00FF0000"
                            )
                            show = f"{remark} Rows: ({start_rows}) in Tmp files."
                        elif start_rows in self.upsert_rows.keys() and remark in [
                            "Inserted",
                            "Updated",
                        ]:
                            sheet.cell(row=start_rows, column=idx).value = values
                            show = f"{remark} Rows: ({start_rows}) in Tmp files. Record Changed: {self.upsert_rows[start_rows]}"
                        else:
                            sheet.cell(row=start_rows, column=idx).value = values
                            show = f"No Change Rows: ({start_rows}) in Tmp files."
                logging.info(show)
                start_rows += 1

            status = "succeed"

        except KeyError as err:
            raise KeyError(f"Can not Write rows: {err} in Tmp files.")
        return status


    def copy_worksheet(self, source_name: str, target_name: str) -> str:
        try:
            if not glob.glob(target_name, recursive=True):
                shutil.copy2(source_name, target_name)
            status = "succeed"
        except FileNotFoundError as err:
            raise FileNotFoundError(err)

        return status


    def remove_row_empty(self, sheet: any) -> None:
        for row in sheet.iter_rows():
            if not all(cell.value for cell in row):
                sheet.delete_rows(row[0].row, 1)
                self.remove_row_empty(sheet)


    def write_data_to_target_file(self) -> None:

        logging.info("Write Data to Target files..")

        source_name = join(Folder.TEMPLATE, "Application Data Requirements.xlsx")
        target_name = join(Folder.EXPORT, Folder._FILE)
        status = "failed"
        start_rows = 2

        for key in self.logging:
            try:
                if key["source"] == "Target_file":
                    key.update(
                        {"function": "write_data_to_target_file", "status": status}
                    )
                    ## read tmp file.
                    tmp_name = key["full_path"]
                    sheet_name = key["sheet_name"]
                    tmp_df = pd.read_excel(tmp_name, sheet_name=sheet_name)
                    tmp_df = tmp_df.loc[tmp_df["remark"] != "Removed"]

                    try:
                        if self.write_mode == "overwrite" or self.manual:
                            target_name = join(Folder.EXPORT, Folder._FILE)
                        else:
                            suffix = f"{self.batch_date.strftime('%d%m%Y')}"
                            Folder._FILE = f"{Path(Folder._FILE).stem}_{suffix}.xlsx"
                            target_name = join(Folder.EXPORT, Folder._FILE)

                        # check files is exist.
                        status = (
                            "succeed"
                            if glob.glob(target_name, recursive=True)
                            else self.copy_worksheet(source_name, target_name)
                        )

                        if status == "succeed":
                            workbook = openpyxl.load_workbook(target_name)
                            get_sheet = workbook.get_sheet_names()
                            sheet = workbook.get_sheet_by_name(get_sheet[0])
                            workbook.active = sheet

                        ## read target file.
                        data = sheet.values
                        columns = next(data)[0:]
                        target_df = pd.DataFrame(data, columns=columns)
                        target_df["remark"] = "Inserted"

                        ## compare data tmp data with target data.
                        select_date = tmp_df["CreateDate"].unique()
                        new_data = self.customize_data(select_date, target_df, tmp_df)

                        key.update({"full_path": target_name})
                    except Exception as err:
                        raise Exception(err)

                    # write data to target files.
                    logging.info(
                        f"Write mode: {self.write_mode} in Target files: '{target_name}'"
                    )
                    max_rows = max(new_data, default=0)

                    while start_rows <= max_rows:
                        for idx, columns in enumerate(new_data[start_rows].keys(), 1):
                            if columns == "remark":
                                if (
                                    f"{start_rows}" in self.upsert_rows.keys()
                                    and new_data[start_rows][columns]
                                    in ["Updated", "Inserted"]
                                ):
                                    show = f"{new_data[start_rows][columns]} Rows: ({start_rows}) in Target files. Record Changed: {self.upsert_rows[f'{start_rows}']}"
                                elif (
                                    start_rows in self.skip_rows
                                    and new_data[start_rows][columns] == "Removed"
                                ):
                                    show = f"{new_data[start_rows][columns]} Rows: ({start_rows}) in Target files."
                                    sheet.delete_rows(start_rows, sheet.max_row)
                                else:
                                    show = f"No Change Rows: ({start_rows}) in Target files."
                            else:
                                if start_rows in self.skip_rows:
                                    continue
                                sheet.cell(row=start_rows, column=idx).value = new_data[
                                    start_rows
                                ][columns]
                                continue
                            logging.info(show)
                        start_rows += 1

                    self.remove_row_empty(sheet)
                    ## save files.
                    workbook.save(target_name)
                    status = "succeed"

                    key.update(
                        {"function": "write_data_to_target_file", "status": status}
                    )
                    logging.info(f"Write to Target Files status: {status}.")

            except Exception as err:
                key.update({"errors": err})

        if "errors" in key:
            raise CustomException(errors=self.logging)
