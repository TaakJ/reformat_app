import logging
from pathlib import Path
from os.path import join
import os
import glob
import shutil
import pandas as pd
import numpy as np
import re
import openpyxl
import chardet
from io import StringIO
from itertools import tee, chain
import xlrd
import csv
from .exception import CustomException
from .setup import Folder

class Convert2File:

    async def check_source_file(self) -> None:
        
        logging.info("Check source file")
        
        for input_dir, full_target in zip(self.full_input, self.full_target):
            if glob.glob(input_dir, recursive=True):
                status = "found"
                err = None
            else:
                status = "not_found"
                err = f"File Not Found {input_dir}"
                    
            record = {"module": self.module,
                    "input_dir": input_dir,
                    "full_target": full_target,
                    "function": "check_source_file",
                    "status": status,}
            
            if err is not None:
                record.update({"err": err})    
            self.logging += [record]
            
            logging.info(f"Check source file: {input_dir}, status: {status}")
            
        self.logging.pop(0)
        if [record for record in self.logging if "err" in record]:
            raise CustomException(err=self.logging)
        
    async def separate_data_file(self) -> None:
        
        logging.info("Separate file from module")
        
        for i, record in enumerate(self.logging):
            record.update({"function": "separate_data_from_file"})
            
            try:
                types = Path(record["input_dir"]).suffix
                status_file = record["status"]
                
                if status_file == "found":
                    if [".xlsx", ".xls"].__contains__(types):
                        self.read_excel_file(i)
                    else:
                        self.read_file(i)
                else:
                    continue

            except Exception as err:
                record.update({"err": err})
            
            if "err" in record:
                raise CustomException(err=self.logging)
            
    def read_excel_file(self, i: int) -> any:

        status = "failed"
        self.logging[i].update({"function": "read_excel_file", "status": status})
        
        try:
            input_dir = self.logging[i]["input_dir"]
            workbook = xlrd.open_workbook(input_dir)
            
            data = self.get_extract_data(i, workbook)

        except Exception as err:
            raise Exception(err)
        
        status = "succeed"
        self.logging[i].update({"status": status})
        
        return data

    def read_file(self, i: int) -> any:

        status = "failed"
        self.logging[i].update({"function": "read_file", "status": status})
        
        try:
            input_dir = self.logging[i]["input_dir"]
            logging.info(f"Read format text/csv file: {input_dir}")
            
            with open(input_dir, 'rb') as f:
                file = f.read()
            encoding_result = chardet.detect(file)
            encoding = encoding_result['encoding']
            line = StringIO(file.decode(encoding))
            
            ## call function collect data in module
            self.get_extract_data(i, line)
            
        except Exception as err:
            raise Exception(err)
        
        status = "succeed"
        self.logging[i].update({"status": status})
    
    def initial_data_type(self, df: pd.DataFrame) -> pd.DataFrame:
        
        try:
            df = df.apply(lambda x: x.str.strip()).replace('', "NA")
            # df[["CreateDate", "LastLogin", "LastUpdatedDate"]] = df[["CreateDate", "LastLogin", "LastUpdatedDate"]]\
            #     .apply(pd.to_datetime, format="%Y%m%d%H%M%S", errors='coerce')
            
            df = df.astype({
                    "ApplicationCode": object,
                    "AccountOwner": object,
                    "AccountName": object,
                    "AccountType": object,
                    "EntitlementName": object,
                    "SecondEntitlementName": object,
                    "ThirdEntitlementName": object,
                    "AccountStatus": object,
                    "IsPrivileged": object,
                    "AccountDescription": object,
                    "CreateDate": object,
                    "LastLogin": object,
                    "LastUpdatedDate": object,
                    "AdditionalAttribute": object,})
            
            if "remark" in df.columns:
                df = df.loc[df["remark"] != "Remove"]
            else:
                df["remark"] = "Insert"

        except Exception as err:
            raise Exception(err)
        
        return df
    
    def initial_param_type(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = df.apply(lambda x: x.str.strip()).replace('', "NA")
            
            df = df.astype({
                    "Parameter Name": object,
                    "Code value": object,
                    "Decode value": object,})
            
            if "remark" in df.columns:
                df = df.loc[df["remark"] != "Remove"]
            else:
                df["remark"] = "Insert"

        except Exception as err:
            raise Exception(err)
        
        return df

    async def genarate_tmp_file(self) -> None:
        
        self.clear_tmp()
        
        logging.info("Genarate Data to Tmp file")

        status = "failed"
        for i, record in enumerate(self.logging):
            try:            
                ## read tmp file
                tmp_dir = join(Folder.TMP, self.module, self.date.strftime("%Y%m%d"))
                os.makedirs(tmp_dir, exist_ok=True)
                tmp_name = f"TMP_{Path(record["full_target"]).stem}.xlsx"
                full_tmp = join(tmp_dir, tmp_name)                
                
                record.update({"input_dir": full_tmp})
                
                ## set dataframe from tmp file 
                self.create_workbook(i)            
                data = self.sheet.values
                columns = next(data)[0:]
                tmp_df = pd.DataFrame(data, columns=columns)
                
                if record["inital_type"] == 2:
                    tmp_df = self.initial_param_type(tmp_df)
                else:
                    tmp_df = self.initial_data_type(tmp_df)
                
                ## set dataframe from raw file      
                raw_df = pd.DataFrame(record["data"])
                
                ## validate data change row by row
                cmp_df = self.compare_data(i, tmp_df, raw_df)
                data_capture = self.data_change_capture(i, cmp_df)
                        
                ## write tmp file
                status = self.write_worksheet(i, data_capture)

            except Exception as err:
                record.update({"err": err})
                
            record.update({"function": "genarate_tmp_file", "status": status})
            logging.info(f"Write Data to Tmp file status: {status}")

            if "err" in record:
                raise CustomException(err=self.logging)
            
    def create_workbook(self, i:int) -> None:

        full_tmp = self.logging[i]["input_dir"]
        logging.info(f"Create Tmp file: {full_tmp}")

        status = "failed"
        self.logging[i].update({"function": "create_workbook", "status": status})

        try:
            self.create = False
            self.workbook = openpyxl.load_workbook(full_tmp)
            get_sheet = self.workbook.get_sheet_names()
            self.sheet_num = len(get_sheet)
            self.sheet_name = f"RUN_TIME_{self.sheet_num - 1}"

            if self.sheet_name in get_sheet:
                self.create = True
                self.sheet = self.workbook.get_sheet_by_name(self.sheet_name)
            else:
                self.sheet = self.workbook.get_sheet_by_name("Field Name")

        except FileNotFoundError:
            template_name = self.logging[i]["template"]
            full_template = join(Folder.TEMPLATE, template_name)
            try:
                if not glob.glob(full_tmp, recursive=True):
                    shutil.copy2(full_template, full_tmp)
            except:
                raise
            
            self.workbook = openpyxl.load_workbook(full_tmp)
            self.sheet = self.workbook.get_sheet_by_name("Field Name")
            self.sheet_name = "RUN_TIME_1"
            self.sheet_num = 1

        status = "succeed"
        self.logging[i].update({"status": status})

    def compare_data(self, i: int, df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        
        logging.info("Compare data")
        
        status = "failed"
        self.logging[i].update({"function": "compare_data", "status": status})
        
        try:
            ## Merge index.
            self.merge_index = np.union1d(df.index, new_df.index)
            ## As starter dataframe for compare
            df = df.reindex(index=self.merge_index, columns=df.columns).iloc[:,:-1]
            ## Change data / new data
            self.new_df = new_df.reindex(index=self.merge_index, columns=new_df.columns).iloc[:,:-1]
            
            ## Compare data
            df["count"] = pd.DataFrame(np.where(df.ne(self.new_df), True, df), index=df.index, columns=df.columns)\
                .apply(lambda x: (x == True).sum(), axis=1)
            
        except Exception as err:
            raise Exception(err)
        
        status = "succeed"
        self.logging[i].update({"status": status})

        return df
    
    def data_change_capture(self, i: int, df: pd.DataFrame) -> dict:
        
        logging.info("Data Change Capture")
        
        status = "failed"
        self.logging[i].update({"function": "data_change_capture", "status": status})
        
        ## set format record
        def format_record(record):
            return "\n".join("{!r} => {!r};".format(columns, values) for columns, values in record.items())
        
        self.update_rows = {}
        self.remove_rows = []
        
        if len(df.index) > len(self.new_df.index):
            self.remove_rows = [idx for idx in list(df.index) if idx not in list(self.new_df.index)]
        
        try:
            i = 0
            for idx, row in enumerate(self.merge_index, 2):
                if row not in self.remove_rows:
                    record = {}
                    for data, change_data in zip(df.items(), self.new_df.items()):
                        ## No Change
                        if df.loc[row, "count"] != 15:
                            if df.loc[row, "count"] < 1:
                                df.loc[row, data[0]] = data[1][row]
                                df.loc[row, "remark"] = "No_change"
                            else:
                                ## Update
                                if data[1][row] != change_data[1][row]:
                                    record.update({data[0]: change_data[1][row]})
                                df.loc[row, data[0]] = change_data[1][row]
                                df.loc[row, "remark"] = "Update"
                        else:
                            ## Insert
                            record.update({data[0]: change_data[1][row]})
                            df.loc[row, data[0]] = change_data[1][row]
                            df.loc[row, "remark"] = "Insert"
                            
                    if record != {}:
                        self.update_rows[idx] = format_record(record)
                else:
                    ## Remove
                    self.remove_rows[i] = idx
                    df.loc[row, "remark"] = "Remove"
                    i += 1
            
            ## set dataframe.
            df = df.drop(["count"], axis=1)
            rows = 2
            df.index += rows
            data_dict = df.to_dict(orient="index")
            
        except Exception as err:
            raise Exception(err)
        
        status = "succeed"
        self.logging[i].update({"status": status})
        
        return data_dict

    def write_worksheet(self, i: int, data_capture: dict) -> str:
        
        # data_capture
        status = "failed"
        if self.create:
            self.sheet_name = f"RUN_TIME_{self.sheet_num}"
            self.sheet = self.workbook.create_sheet(self.sheet_name)

        logging.info(f"Write to Sheet: {self.sheet_name}")

        # rows = 2
        # max_row = max(change_data, default=0)
        # self.logging[-1].update({"function": "write_worksheet", "sheet_name": self.sheet_name, "status": status,})
        
        # try:
        #     # write column
        #     for idx, col in enumerate(change_data[rows].keys(), 1):
        #         self.sheet.cell(row=1, column=idx).value = col

        #     ## write row
        #     while rows <= max_row:
        #         for idx, col in enumerate(change_data[rows].keys(), 1):

        #             if col in ["CreateDate", "LastLogin", "LastUpdatedDate"]:
        #                 change_data[rows][col] = change_data[rows][col].strftime("%Y%m%d%H%M%S")
                        
        #             self.sheet.cell(row=rows, column=idx).value = change_data[rows][col]

        #             if col == "remark":
        #                 if rows in self.remove_rows:
        #                     ## Remove row
        #                     write_row = (f"{change_data[rows][col]} Rows: ({rows}) in Tmp file")
        #                 elif rows in self.update_rows.keys():
        #                     ## Update / Insert row
        #                     write_row = f"{change_data[rows][col]} Rows: ({rows}) in Tmp file Updating records: {self.update_rows[rows]}"
        #                 else:
        #                     ## No change row
        #                     write_row = f"No Change Rows: ({rows}) in Tmp file"
        #                 logging.info(write_row)
        #         rows += 1

        # except KeyError as err:
        #     raise KeyError(f"Can not Write rows: {err} in Tmp file")

        # ## save file
        # full_tmp = self.logging[-1]["input_dir"]
        # self.sheet.title = self.sheet_name
        # self.workbook.active = self.sheet
        # self.workbook.move_sheet(self.workbook.active, offset=-self.sheet_num)
        # self.workbook.save(full_tmp)

        # status = "succeed"
        # self.logging[-1].update({"status": status})

        # return status

    async def genarate_target_file(self) -> None:

        logging.info("Genarate Data to Target file")

        status = "failed"
        for record in self.logging:
            try:

                if record["module"] == "Target_file":
                    try:
                        ## read tmp file or dataframe
                        if self.store_tmp is True:
                            full_tmp = record["input_dir"]
                            sheet_name = record["sheet_name"]
                            change_df = pd.read_excel(full_tmp, sheet_name=sheet_name, dtype=object)
                        else:
                            data = record["data"]
                            change_df = pd.DataFrame(data)
                        change_df = self.initial_data_type(change_df)

                        ## read csv file
                        try:
                            target_df = self.read_csv_file(self.full_target)
                            
                        except FileNotFoundError:
                            ## move from template file to target file
                            template_name = join(Folder.TEMPLATE, "Application Data Requirements.xlsx")
                            target_df = pd.read_excel(template_name)
                            target_df.to_csv(self.full_target, index=None, header=True, sep=",")
                            
                        ## optimize data
                        data = self.optimize_data(target_df, change_df)
                        
                        ## write csv file
                        status = self.write_csv(self.full_target, data)

                    except Exception as err:
                        raise Exception(err)

                    record.update({"function": "genarate_target_file", "state": status})
                    logging.info(f"Genarate to Target file status: {status}")

            except Exception as err:
                record.update({"err": err})

        if "err" in record:
            raise CustomException(err=self.logging)
        
    def read_csv_file(self, file: str) -> pd.DataFrame:
        
        logging.info(f"Read csv file: {file}")
        
        status = "failed"
        self.logging[-1].update({"input_dir": self.full_target, "function": "read_csv", "status": status})
        
        data = []
        with open(file, "r", newline="\n") as reader:
            csv_reader = csv.reader(
                reader,
                skipinitialspace=True,
                delimiter=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL)
            
            header = next(csv_reader)
                
            ## skip last row (total row)
            last_row, row = tee(csv_reader)
            next(chain(last_row, range(1)))
            for _ in last_row:
                data.append(next(row))
                    
        df = pd.DataFrame(data, columns=header)
        
        status = "succeed"
        self.logging[-1].update({"status": status})

        return df

    def optimize_data(self, target_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:

        logging.info("Optimize Data Before Write to Target")

        status = "failed"
        self.logging[-1].update({"function": "optimize_data", "status": status})

        data = {}
        try:
            target_df = self.initial_data_type(target_df)

            ## Validate data change row by row
            df = target_df[target_df["CreateDate"].isin(np.array([pd.Timestamp(self.batch_date)]))].reset_index(drop=True)
            cmp_df = self.compare_data(df, new_df)
            data_capture = self.data_change_capture(cmp_df)
            
            ## merge data with data_capture and target
            merge_data = target_df[~target_df["CreateDate"].isin(np.array([pd.Timestamp(self.batch_date)]))].iloc[:,:-1].to_dict("index")
            max_row = max(merge_data, default=0)
            for idx, values in data_capture.items():
                if idx in self.update_rows or idx in self.remove_rows:
                    values.update({"mark_row": idx})
                merge_data = {**merge_data, **{max_row + idx: values}}
            
            ## sorted order data on batch date
            i = 0
            for idx, values in enumerate(sorted(merge_data.values(), key=lambda d: d["CreateDate"]), 2):  # LastLogin
                if "mark_row" in values.keys():
                    if values["mark_row"] in self.update_rows:
                        self.update_rows[idx] = self.update_rows.pop(values["mark_row"])
                    else:
                        self.remove_rows[i] = idx
                        i += 1
                    values.pop("mark_row")
                data.update({idx: values})

        except Exception as err:
            raise Exception(err)

        status = "succeed"
        self.logging[-1].update({"status": status})

        return data

    def write_csv(self, target_name: str, data: dict) -> str:

        logging.info(f"Write mode: {self.write_mode} in Target file: {target_name}")

        status = "failed"
        self.logging[-1].update({"function": "write_csv", "status": status})

        try:
            with open(target_name, "r", newline="\n") as reader:
                csvin = csv.DictReader(
                    reader,
                    skipinitialspace=True,
                    delimiter=",",
                    quotechar='"',
                    quoting=csv.QUOTE_ALL,
                )
                
                ## skip last row (total row)
                last_row, row = tee(csvin)
                next(chain(last_row, range(1)))
            
                rows = {}
                for idx , _ in enumerate(last_row, 2):
                    rows.update({idx: next(row)})
                
                for idx, value in data.items():
                    if value.get("remark") is not None:
                        if idx in self.update_rows.keys():
                            logging.info(f'{value["remark"]} Rows: ({idx}) in Target file Updating records: {self.update_rows[idx]}')
                            value.popitem()
                            rows.update({idx: value})
                            
                        elif idx in self.remove_rows:
                            continue
                    else:
                        rows[idx] = data[idx]
            
            with open(target_name, "w", newline="\n") as writer:
                csvout = csv.DictWriter(
                    writer,
                    csvin.fieldnames,
                    delimiter=",",
                    quotechar='"',
                    quoting=csv.QUOTE_ALL,
                )
                csvout.writeheader()
            
                for idx in rows:
                    if idx not in self.remove_rows:
                        rows[idx].update({
                            "CreateDate": rows[idx]["CreateDate"].strftime("%Y%m%d%H%M%S"),
                            "LastLogin": rows[idx]["LastLogin"].strftime("%Y%m%d%H%M%S"),
                            "LastUpdatedDate": rows[idx]["LastUpdatedDate"].strftime("%Y%m%d%H%M%S"),
                            })
                        csvout.writerow(rows[idx])
                writer.close()
            
            with open(target_name, mode="a", newline="\n") as writer:
                writer.write('"{}","{}"'.format("TotalCount", len(rows)))
                logging.info(f'Total count: ({len(rows)}) row')
                writer.close()
            
        except Exception as err:
            raise Exception(err)

        status = "succeed"
        self.logging[-1].update({"status": status})

        return status
