import logging
import re
import traceback
import pandas as pd
from .exception import CustomException
from .non_functional import CallFunction

class ModuleDIL(CallFunction):

    def __init__(self, params: any) -> None:
        for key, value in vars(params).items():
            setattr(self, key, value)

    def logSetter(self, log: list) -> None:
        self._log = log

    async def run_process(self) -> dict:

        # Initialize the logger
        logging.getLogger(__name__)
        logging.info(f"Module:'{self.module}'; Manual: '{self.manual}'; Run date: '{self.batch_date}'; Store tmp: '{self.store_tmp}'; Write mode: '{self.write_mode}';")
        
        try:
            # Collect setup
            self.collect_setup()
            # Clear target files
            self.clear_target_file()
            # Task check source file
            await self.check_source_file()
            # Task separate data file
            await self.separate_data_file()
            # Task generate tmp files
            if self.store_tmp is True:
                await self.generate_tmp_file()
            # Task generate target files
            await self.generate_target_file()

        except CustomException as err:
            # Log error details
            logging.error("See Error details at log_error.log")
            logger = err.setup_errorlog(log_name=__name__)
            
            while True:
                try:
                    logger.error(next(err))
                except StopIteration:
                    break

        logging.info(f"Stop Run Module '{self.module}'\n")

    def collect_user_file(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({"function": "collect_user_file", "status": status})

        try:
            clean_data = self.read_format_file(format_file)
            
            # verify data length 
            self.validate_row_length(clean_data)

            # Creating DataFrame
            user_df = pd.DataFrame(clean_data)
            user_df.columns = user_df.iloc[0].values
            user_df = (user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True))
            user_df = user_df[user_df["APPCODE"] == "LNSIGNET"].reset_index(drop=True)
            user_df = user_df.map(lambda row: ("NA" if isinstance(row, str) and (row.lower() == "null" or row == "") else row))

            # Adjusting Columns
            user_df[["NAME", "DEPARTMENT"]] = user_df.apply(self.split_column, axis=1, result_type="expand")
            user_df["ATTRIBUTE"] = user_df.apply(self.attribute_column, axis=1)

            # Mapping Data to Target Columns
            target_columns  = self.logging[i]["columns"]
            mapping = dict.fromkeys(target_columns, "NA")
            mapping.update(
                {
                    "ApplicationCode": "DIL",
                    "AccountOwner": user_df["USERNAME"],
                    "AccountName": user_df["USERNAME"],
                    "AccountType": "USR",
                    "AccountStatus": "A",
                    "IsPrivileged": "N",
                    "AccountDescription": user_df["NAME"],
                    "AdditionalAttribute": user_df[["DEPARTMENT", "APPCODE", "ATTRIBUTE"]].apply(lambda row: "#".join(row), axis=1),
                    "Country": "TH",
                }
            )
            user_df = user_df.assign(**mapping)
            user_df = user_df.drop(user_df.iloc[:, :12].columns, axis=1)

        except Exception as err:
            # Extract the traceback to get error details
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)

        status = "succeed"
        self.logging[i].update({"data": user_df.to_dict("list"), "status": status})
        logging.info(f"Collect user data, status: {status}")

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({"function": "collect_param_file", "status": status})

        try:
            clean_data = self.read_format_file(format_file)
            
            # verify data length 
            self.validate_row_length(clean_data)

            # Creating DataFrame
            param_df = pd.DataFrame(clean_data)
            param_df.columns = param_df.iloc[0].values
            param_df = (param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True))
            param_df = param_df[param_df["APPCODE"] == "LNSIGNET"].reset_index(drop=True)
            param_df = param_df.map(lambda row: ("NA"if isinstance(row, str) and (row.lower() == "null" or row == "") else row))

            # Adjusting Columns
            param_df[["NAME", "DEPARTMENT"]] = param_df.apply(self.split_column, axis=1, result_type="expand")

            # Mapping Data to Target Columns
            mapping = [
                {
                    "Parameter Name": "Department",
                    "Code values": param_df["DEPARTMENT"].unique(),
                    "Decode value": param_df["DEPARTMENT"].unique(),
                },
                {
                    "Parameter Name": "AppCode",
                    "Code values": "LNSIGNET",
                    "Decode value": "LNSIGNET",
                },
                {
                    "Parameter Name": "Role",
                    "Code values": ["Inquiry", "Admin", "Index + Scan"],
                    "Decode value": ["Inquiry", "Admin", "Index + Scan"],
                },
            ]
            param_df = pd.DataFrame(mapping)
            param_df = param_df.explode(["Code values", "Decode value"]).reset_index(drop=True)

        except Exception as err:
            # Extract the traceback to get error details
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)

        status = "succeed"
        self.logging[i].update({"data": param_df.to_dict("list"), "status": status})
        logging.info(f"Collect param data, status: {status}")

    def read_format_file(self, format_file) -> list:

        data = [re.sub(r"(?<!\.)\s{3,}", "||", "".join(re.findall(r"\w+.*", line.strip()))).split("||") for line in format_file if re.findall(r"\w+.*", line.strip())]

        clean_data = []
        for rows, _data in enumerate(data):
            if rows == 4:
                clean_data += [re.sub(r"\s+", ",", ",".join(_data)).split(",")]
            elif rows >= 5:
                fix_value = []
                for idx, value in enumerate(_data, 1):
                    if idx == 4:
                        value = re.sub(r"\s+", ",", value).split(",")
                        fix_value.extend(value)
                    else:
                        fix_value.append(value)
                clean_data.append(fix_value)
            else:
                continue

        return clean_data
    
    def validate_row_length(self, rows_list: list[list], expected_length: int=10) -> None:
        # Assert that the length of the row matches the expected length
        errors = []
        for i, rows in enumerate(rows_list, 7):
            try:
                assert (len(rows) == expected_length or len(rows) == 1), f"Row {i} has data invalid. value:{rows}"
            except AssertionError as err:
                errors.append(str(err))
                
        if errors:
            raise Exception("\n".join(errors))
        
    def split_column(self, row: any) -> any:
        comma = row["NAME"].split(",")
        if len(comma) == 3:
            name, department, _ = comma
        elif len(comma) == 2:
            name, department = comma
        else:
            name = row["NAME"]
            department = "NA"

        return name.strip(), department.strip()

    def attribute_column(self, row: any) -> str:
        if (row["ADD_ID"] == "0" and row["EDIT_ID"] == "0" and row["ADD_DOC"] == "0" and row["EDIT_DOC"] == "0" and row["SCAN"] == "0" and row["ADD_USER"] == "1"):
            return "Admin"
        elif (row["ADD_ID"] == "1" and row["EDIT_ID"] == "1" and row["ADD_DOC"] == "1" and row["EDIT_DOC"] == "1" and row["SCAN"] == "1" and row["ADD_USER"] == "0"):
            return "Index+Scan"
        else:
            return "Inquiry"