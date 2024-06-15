from exception import CustomException
from setup import Folder
import logging
from pathlib import Path
from os.path import join
import glob
import shutil
import pandas as pd
import numpy as np
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
        for _dir in self.input_dir:

            status_file = "not_found"
            if glob.glob(_dir, recursive=True):
                status_file = "found"

            record = {
                "module": self.module,
                "input_dir": _dir,
                "status_file": status_file,
                "function": "check_source_files",
            }
            set_log.append(record)
            logging.info(f'Source file: "{_dir}", Status: "{status_file}"')

        self._log_setter(set_log)


    async def retrieve_data_from_source_files(self) -> None:

        logging.info("Retrieve Data from Source files..")

        state = "failed"
        for i, record in enumerate(self.logging):
            record.update({"function": "retrieve_data_from_source_files", "state": state})

            _dir = record["input_dir"]
            types = Path(_dir).suffix
            status_file = record["status_file"]
            try:
                if status_file == "found":
                    if [".xlsx", ".xls"].__contains__(types):
                        logging.info(f'Read Excel file: "{_dir}"')
                        _data = self.excel_data_cleaning(i)

                    else:
                        logging.info(f'Read Text file: "{_dir}"')
                        _data = self.text_data_cleaning(i)
                else:
                    continue
                state = "succeed"
                record.update({"data": _data, "state": state})

            except Exception as err:
                record.update({"errors": err})

            if "errors" in record:
                raise CustomException(errors=self.logging)
            

    def read_text_files(func):
        def wrapper(*args: tuple, **kwargs: dict) -> dict:

            by_lines = iter(func(*args, **kwargs))
            _data = {}

            rows = 0
            while True:
                try:
                    list_by_lines = []
                    for source, data in next(by_lines).items():

                        if source == "LDS":
                            if rows == 0:
                                ## herder column
                                list_by_lines = " ".join(data).split(" ")
                            else:
                                ## row value
                                for idx, value in enumerate(data):
                                    if idx == 0:
                                        value = re.sub(r"\s+", ",", value).split(",")
                                        list_by_lines.extend(value)
                                    else:
                                        list_by_lines.append(value)

                        elif source == "DOC":
                            if rows == 1:
                                ## herder column
                                list_by_lines = " ".join(data).split(" ")
                            elif rows > 1:
                                ## row value
                                for idx, value in enumerate(data):
                                    if idx == 3:
                                        value = re.sub(r"\s+", ",", value).split(",")
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
    def text_data_cleaning(self, i: int) -> any:

        # logging.info("Cleansing Data From Text file..")
        self.logging[i].update({"function": "text_data_cleaning"})

        _dir = self.logging[i]["input_dir"]
        source = self.logging[i]["source"]

        files = open(_dir, "rb")
        encoded = chardet.detect(files.read())["encoding"]
        files.seek(0)
        decode_data = StringIO(files.read().decode(encoded))

        for lines in decode_data:
            regex = re.compile(r"\w+.*")
            find_regex = regex.findall(lines)
            if find_regex != []:
                yield {source: re.sub(r"\W\s+", "||", ""\
                    .join(find_regex).strip()).split("||")}


    def read_excle_files(func):
        def wrapper(*args: tuple, **kwargs: dict) -> dict:

            by_sheets = iter(func(*args, **kwargs))
            _data = {}

            while True:
                try:
                    for sheets, data in next(by_sheets).items():
                        if not all(dup == data[0] for dup in data) and not data\
                            .__contains__("Centralized User Management : User List."):
                            if sheets not in _data:
                                _data[sheets] = [data]
                            else:
                                _data[sheets].append(data)

                except StopIteration:
                    break
            return _data

        return wrapper


    @read_excle_files
    def excel_data_cleaning(self, i: int) -> any:

        # logging.info("Cleansing Data From Excle files..")
        self.logging[i].update({"function": "excel_data_cleaning"})

        workbook = xlrd.open_workbook(self.logging[i]["input_dir"])
        sheet_list = [sheet for sheet in workbook.sheet_names() if sheet != "StyleSheet"]

        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                yield {sheets: [cells.cell(row, col)\
                    .value for col in range(cells.ncols)]}


    async def write_data_to_tmp_file(self) -> None:

        logging.info("Write Data to Tmp files..")

        template_name = join(Folder.TEMPLATE, "Application Data Requirements.xlsx")
        tmp_name = join(Folder.TMP, f"TMP_{self.source}-{self.batch_date.strftime('%d%m%y')}.xlsx")

        state = "failed"
        for record in self.logging:
            try:
                if record["source"] == "Target_file":
                    record.update(
                        {
                            "input_dir": tmp_name,
                            "function": "write_data_to_tmp_file",
                            "state": state,
                        }
                    )
                    ## get new data.
                    change_df = pd.DataFrame(record["data"])
                    change_df["remark"] = "Inserted"

                    try:
                        workbook = openpyxl.load_workbook(tmp_name)
                        get_sheet = workbook.get_sheet_names()
                        sheet_num = len(get_sheet)
                        sheet_name = f"RUN_TIME_{sheet_num - 1}"
                        sheet = workbook.get_sheet_by_name(sheet_name)
                        workbook.active = sheet_num

                    except FileNotFoundError:
                        ## copy files from template.
                        state = self.copy_worksheet(template_name, tmp_name)
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

                    if state != "succeed":
                        ## create new sheet.
                        tmp_df = tmp_df.loc[tmp_df["remark"] != "Removed"]
                        sheet_name = f"RUN_TIME_{sheet_num}"
                        sheet = workbook.create_sheet(sheet_name)
                    else:
                        tmp_df["remark"] = "Inserted"

                    _data = self.validation_data(tmp_df, change_df)
                    ## write to tmp files.
                    state = self.write_worksheet(sheet, _data)
                    workbook.move_sheet(workbook.active, offset=-sheet_num)
                    workbook.save(tmp_name)

                    record.update({"sheet_name": sheet_name, "state": state})
                    logging.info(f"Write to Tmp files state: {state}.")

            except Exception as err:
                record.update({"errors": err})

            if "errors" in record:
                raise CustomException(errors=self.logging)


    def validation_data(self, df: pd.DataFrame, change_df: pd.DataFrame) -> dict:

        logging.info("Verify Changed information..")
        self.logging[-1].update({"function": "validation_data"})

        self.change_rows = {}
        self.remove_rows = []
        if len(df.index) > len(change_df.index):
            self.remove_rows = [
                idx for idx in list(df.index) if idx not in list(change_df.index)
            ]

        ## reset index data.
        union_index = np.union1d(df.index, change_df.index)
        ## target / tmp data.
        df = df.reindex(index=union_index, columns=df.columns).iloc[:, :-1]
        ## change data.
        change_df = change_df.reindex(index=union_index,columns=change_df.columns).iloc[:, :-1]

        # compare data rows by rows.
        df["count_change"] = pd.DataFrame(
            np.where(df.ne(change_df), True, False), index=df.index, columns=df.columns
        ).apply(lambda x: (x == True).sum(), axis=1)

        def format_record(record):
            return ("{"+ "\n".join("{!r}: {!r},".format(columns, values)\
                for columns, values in record.items())+"}")

        start_rows = 2
        for idx in union_index:
            if idx not in self.remove_rows:

                record = {}
                for data, change_data in zip(df.items(), change_df.items()):
                    ## data[0] = column_name
                    ## data[1] = value
                    if df.loc[idx, "count_change"] != 14:

                        ## No_changed rows.
                        if df.loc[idx, "count_change"] <= 1:
                            df.at[idx, data[0]] = data[1].iloc[idx]
                            df.loc[idx, "remark"] = "No_changed"

                        else:
                            ## Updated rows.
                            if data[1][idx] != change_data[1][idx]:
                                record.update({data[0]: f"{data[1][idx]} \
                                    -> {change_data[1][idx]}"})
                            df.at[idx, data[0]] = change_data[1].iloc[idx]
                            df.loc[idx, "remark"] = "Updated"

                    else:
                        ## Inserted rows.
                        record.update({data[0]: change_data[1][idx]})
                        df.at[idx, data[0]] = change_data[1].iloc[idx]
                        df.loc[idx, "remark"] = "Inserted"

                if record != {}:
                    self.change_rows[start_rows + idx] = format_record(record)

            else:
                ## Removed rows.
                df.loc[idx, "remark"] = "Removed"
        self.remove_rows = [idx + start_rows for idx in self.remove_rows]

        df = df.drop(["count_change"], axis=1)
        df.index += start_rows
        _data = df.to_dict("index")

        self.logging[-1].update({"status": "verify"})
        return _data


    def write_worksheet(self, sheet: any, change_data: dict) -> str:

        self.logging[-1].update({"function": "write_worksheet"})
        max_rows = max(change_data, default=0)
        logging.info(f"Data for write: {max_rows}. rows")

        start_rows = 2
        try:
            # write columns.
            for idx, columns in enumerate(change_data[start_rows].keys(), 1):
                sheet.cell(row=1, column=idx).value = columns
            ## write data.
            while start_rows <= max_rows:
                for remark in [
                    change_data[start_rows][columns]
                    for columns in change_data[start_rows].keys()
                    if columns == "remark"
                ]:
                    for idx, values in enumerate(change_data[start_rows].values(), 1):

                        if start_rows in self.remove_rows and remark == "Removed":
                            ## Removed
                            sheet.cell(row=start_rows, column=idx).value = values
                            sheet.cell(row=start_rows, column=idx).font = Font(bold=True,\
                                strike=True, color="00FF0000")
                            show = f"{remark} Rows: ({start_rows}) in Tmp files."

                        elif start_rows in self.change_rows.keys() and remark in ["Inserted","Updated"]:
                            ## Updated / Insert
                            sheet.cell(row=start_rows, column=idx).value = values
                            show = f"{remark} Rows: ({start_rows}) in Tmp files. Record Changed: {self.change_rows[start_rows]}"

                        else:
                            ## no change
                            sheet.cell(row=start_rows, column=idx).value = values
                            show = f"No Change Rows: ({start_rows}) in Tmp files."

                logging.info(show)
                start_rows += 1

            state = "succeed"
        except KeyError as err:
            raise KeyError(f"Can not Write rows: {err} in Tmp files.")
        return state


    def copy_worksheet(self, source_name: str, target_name: str) -> str:
        try:
            if not glob.glob(target_name, recursive=True):
                shutil.copy2(source_name, target_name)
            state = "succeed"
        except FileNotFoundError as err:
            raise FileNotFoundError(err)
        return state


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
                                    f"{start_rows}" in self.change_rows.keys()
                                    and new_data[start_rows][columns]
                                    in ["Updated", "Inserted"]
                                ):
                                    show = f"{new_data[start_rows][columns]} Rows: ({start_rows}) in Target files. Record Changed: {self.change_rows[f'{start_rows}']}"
                                elif (
                                    start_rows in self.remove_rows
                                    and new_data[start_rows][columns] == "Removed"
                                ):
                                    show = f"{new_data[start_rows][columns]} Rows: ({start_rows}) in Target files."
                                    sheet.delete_rows(start_rows, sheet.max_row)
                                else:
                                    show = f"No Change Rows: ({start_rows}) in Target files."
                            else:
                                if start_rows in self.remove_rows:
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
