from exception import CustomException
from setup import Folder, clear_tmp, setup_config
from log import call_logging
import glob
import shutil
from pathlib import Path
from os.path import join
import warnings
import logging
import pandas as pd
from datetime import datetime
import openpyxl
from openpyxl.styles import Font


class convert_2_files(call_logging):
    def __init__(self, params: dict):
        super().__init__()

        self.__dict__.update(params)
        for key, value in self.__dict__.items():
            setattr(self, key, value)

        logging.info(f"Command for run: {params}")
        logging.info(f"Start run batch date: {self.batch_date}")

        self.date = datetime.now()
        self.skip_rows = []
        self.upsert_rows = {}

        self.state = True
        try:
            if self.store_tmp is False:
                clear_tmp()

            # self.check_source_files()
            # self.retrieve_data_from_source_files()
            # self.write_data_to_tmp_file()
            # self.write_data_to_target_file()

        except CustomException as errors:
            self._status = False
            logging.error("Error Exception")
            while True:
                try:
                    msg_err = next(errors)
                    logging.error(msg_err)
                except StopIteration:
                    break
        logging.info(f"stop batch date: {self.batch_date}\n##### End #####\n")

    def _log_setter(self, log):
        self._log = log

    def check_source_files(self) -> None:

        logging.info("Check Source files..")
        
        map_item = []
        for source, value in self.config["config"].items():
            if source in self.source:
                
                status_file = "not_found"
                for dir_path in value["dir_path"]:
                    if glob.glob(dir_path, recursive=True):
                        status_file = "found"
                        
                    item = {"source": source, 
                            "dir_path": dir_path, 
                            "status_file": status_file,
                            "dir_output": value["dir_output"], 
                            "function": "check_source_files"}
                        
                    map_item.append(item)
        
        for x in map_item:
            print(x)
        
        self._log_setter(map_item)

    def retrieve_data_from_source_files(self) -> list[dict]:

        logging.info("Retrieve Data from Source files..")
        state = "failed"

        for i, item in enumerate(self.logging):
            print(item)
            item.update({'function': "retrieve_data_from_source_files", 'state': state})
            
            # dir_path = item['dir_path']
            # print(dir_path)
            # types = Path(item['dir_path']).suffix
            # status_file = item['status_file']
            
            # try:
            #     if status_file == "found":
            #         if ['.xlsx', '.xls'].__contains__(types):
            #             logging.info(f'Read Excel file: "{dir_path}".')
            #             clean_data = self.generate_excel_data(i)
                        
            #         else:
            #             logging.info(f'Read Text file: "{dir_path}".')
            #             print(dir_path)
            #             # clean_data = self.generate_text_data(i)
            #     else:
            #         continue
                
            # print(clean_data)
                # state = "succeed"
                # item.update({'state': state, 'data': clean_data})

            # except Exception as err:
            #    item.update({'errors': err})

            # if "errors" in key:
            #     raise CustomException(errors=self.logging)

        return self.logging

    # def mapping_data(self):
    #     for key in self.logging:
    #         print(f"source: {key['source']}")
    #         for sheet, data in key['data'].items():
    #             print(f"sheet: {sheet}")
    #             df = pd.DataFrame(data)
    #             # df.columns = df.iloc[0].values
    #             # df = df[1:]
    #             # df = df.reset_index(drop=True)
    #             print(df)

    def write_data_to_tmp_file(self) -> None:

        logging.info("Write Data to Tmp files..")

        source_name = join(Folder.TEMPLATE, "Application Data Requirements.xlsx")
        tmp_name = join(Folder.TMP, f"TMP_{self.batch_date.strftime('%d%m%Y')}.xlsx")
        status = "failed"

        for key in self.logging:
            try:
                if key["source"] == "Target_file":
                    key.update(
                        {
                            "full_path": tmp_name,
                            "function": "write_data_to_tmp_file",
                            "status": status,
                        }
                    )
                    ## get new data.
                    new_df = pd.DataFrame(key["data"])
                    new_df["remark"] = "Inserted"
                    try:
                        workbook = openpyxl.load_workbook(tmp_name)
                        get_sheet = workbook.get_sheet_names()
                        sheet_num = len(get_sheet)
                        sheet_name = f"RUN_TIME_{sheet_num - 1}"
                        sheet = workbook.get_sheet_by_name(sheet_name)
                        workbook.active = sheet_num

                    except FileNotFoundError:
                        ## copy files from template.
                        status = self.copy_worksheet(source_name, tmp_name)
                        workbook = openpyxl.load_workbook(tmp_name)
                        sheet = workbook.worksheets[0]
                        sheet_name = "RUN_TIME_1"
                        sheet_num = 1
                        sheet.title = sheet_name

                    logging.info(f"Generate Sheet_name: {sheet_name} in Tmp files.")

                    # read tmp files.
                    data = sheet.values
                    columns = next(data)[0:]
                    tmp_df = pd.DataFrame(data, columns=columns)

                    if status != "succeed":
                        tmp_df = tmp_df.loc[tmp_df["remark"] != "Removed"]
                        ## create new sheet.
                        sheet_name = f"RUN_TIME_{sheet_num}"
                        sheet = workbook.create_sheet(sheet_name)
                    else:
                        tmp_df["remark"] = "Inserted"

                    new_data = self.validation_data(tmp_df, new_df)
                    ## write to tmp files.
                    status = self.write_worksheet(sheet, new_data)
                    workbook.move_sheet(workbook.active, offset=-sheet_num)
                    workbook.save(tmp_name)

                    key.update({"sheet_name": sheet_name, "status": status})
                    logging.info(f"Write to Tmp files status: {status}.")

            except Exception as err:
                key.update({"errors": err})

            if "errors" in key:
                raise CustomException(errors=self.logging)

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
