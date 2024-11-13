import re
import pandas as pd
import logging
from .non_functional import CallFunction
from .exception import CustomException


class ModuleLDS(CallFunction):

    def __init__(self, params: any) -> None:
        for key, value in vars(params).items():
            setattr(self, key, value)

    def logSetter(self, log: list) -> None:
        self._log = log

    async def run_process(self) -> dict:

        logging.info(f"Module:'{self.module}'; Manual: '{self.manual}'; Run date: '{self.batch_date}'; Store tmp: '{self.store_tmp}'; Write mode: '{self.write_mode}';")

        result = {"module": self.module, "task": "Completed"}
        try:
            self.collect_setup()
            self.clear_target_file()

            await self.check_source_file()
            await self.separate_data_file()
            if self.store_tmp is True:
                await self.generate_tmp_file()
            await self.generate_target_file()

        except CustomException as err:
            logging.error("See Error Details: log_error.log")

            logger = err.setup_errorlog(log_name=__name__)
            while True:
                try:
                    logger.error(next(err))
                except StopIteration:
                    break

            result.update({"task": "Uncompleted"})

        logging.info(f"Stop Run Module '{self.module}'\r\n")

        return result

    def parse_datetime(self, date_str):
        formats = [
            "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S.%f",
            "%d/%m/%Y %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"
            ]
        for fmt in formats:
            try:
                parsed_date = pd.to_datetime(date_str, format=fmt)
                if parsed_date.year < 2000:
                    return pd.NaT
                return parsed_date
            except ValueError:
                continue
        return pd.NaT

    def validate_row_length(self, rows_list: list[list], valid_lengths: list[int] = [3, 32, 33]) -> None:
        errors = []
        for i, rows in enumerate(rows_list, 2):
            try:
                assert (len(rows) in valid_lengths), f"row {i} does not match values {rows}"
            except AssertionError as err:
                errors.append(str(err))
        if errors:
            raise Exception("Data issue: " + "\n".join(errors))

    def read_format_file(self, format_file) -> list:
        
        def clean_and_split_line(line):
            # Cleans and splits a line, handling quoted sections and extra spaces.
            cleaned_line = re.sub(r"\s+", " ", line.strip())  # Remove extra spaces
            split_line = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', cleaned_line)
            return [x.strip() for x in split_line]

        data = []
        for line in format_file:
            if re.findall(r"\w+.*", line.strip()):
                data.append(clean_and_split_line(line))

        clean_data = []
        for row_index, row in enumerate(data):
            if row_index == 0:
                # Process the header row
                header_row = re.sub(r"\s+", ",", ",".join(row)).split(",")
                clean_data.append(header_row)
            else:
                # Process subsequent rows
                cleaned_row = []
                for col_index, value in enumerate(row, 1):
                    if col_index == 1:
                        split_values = re.sub(r"\s+", ",", value).split(",")
                        cleaned_row.extend(split_values)
                    else:
                        cleaned_row.append(value)
                clean_data.append(cleaned_row)
                
        return clean_data

    def collect_user_file(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({"function": "collect_user_file", "status": status})

        try:
            clean_data = self.read_format_file(format_file)
            self.validate_row_length(clean_data)

            # Creating DataFrame
            user_df = pd.DataFrame(clean_data)
            user_df.columns = user_df.iloc[0].values
            user_df = (user_df.iloc[1:-1, :-1].apply(lambda row: row.str.strip()).reset_index(drop=True))
            user_df = user_df.map(lambda row: ("NA" if isinstance(row, str) and (row.lower() == "null" or row == "") else row))
            
            # Adjust column: CostCenterName
            user_df['CostCenterName'] = user_df['CostCenterName'].apply(lambda row: ' '.join(row.replace('.', ' ').replace(',', ' ').split()).strip())
            
            # Mapping Data to Target Columns
            target_columns = self.logging[i]["columns"]
            mapping  = dict.fromkeys(target_columns, "NA")
            mapping.update(
                {
                    "ApplicationCode": "LDS",
                    "AccountOwner": user_df["UserName"],
                    "AccountName": user_df["UserName"],
                    "AccountType": "USR",
                    "EntitlementName": user_df["RoleID"],
                    "AccountStatus": user_df["User_Active"].apply(lambda row: "A" if row.lower() == "active" else "D"),
                    "IsPrivileged": "N",
                    "AccountDescription": user_df["FullName"],
                    "LastLogin": user_df["LastLogin_Date"].apply(self.parse_datetime).dt.strftime("%Y%m%d%H%M%S"),
                    "LastUpdatedDate": user_df["edit_date"].apply(self.parse_datetime).dt.strftime("%Y%m%d%H%M%S"),
                    "AdditionalAttribute": user_df[["CostCenterName", "CostCenterCode"]].apply(lambda row: "#".join(row), axis=1),
                    "Country": "TH",
                }
            )
            user_df = user_df.assign(**mapping)
            user_df = user_df.drop(user_df.iloc[:, :32].columns, axis=1)

        except:
            raise

        status = "succeed"
        self.logging[i].update({"data": user_df.to_dict("list"), "status": status})
        logging.info(f"Collect user data, status: {status}")

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({"function": "collect_param_file", "status": status})

        try:
            clean_data = self.read_format_file(format_file)
            self.validate_row_length(clean_data)

            # Creating DataFrame
            param_df = pd.DataFrame(clean_data)
            param_df.columns = param_df.iloc[0].values
            param_df = (param_df.iloc[1:-1, :-1].apply(lambda row: row.str.strip()).reset_index(drop=True))
            param_df = param_df.map(lambda row: ("NA" if isinstance(row, str) and (row.strip().lower() == "null" or row.strip() == "") else row))
            
            # Adjust column: CostCenterName
            param_df['CostCenterName'] = param_df['CostCenterName'].apply(lambda row: ' '.join(row.replace('.', ' ').replace(',', ' ').split()).strip())
            
            # Mapping Data to Target Columns
            target_columns = self.logging[i]["columns"]
            merge_df = pd.DataFrame(columns=target_columns)
            
            # Extract unique RoleID and RoleName
            unique_roles = param_df[['RoleID', 'RoleName']].drop_duplicates()
            role_params = pd.DataFrame({
                'Parameter Name': 'Role',
                'Code values': unique_roles['RoleID'],
                'Decode value': unique_roles['RoleName']
            })
            
            # Extract unique CostCenterName
            unique_dept = param_df['CostCenterName'].unique()
            dept_params = pd.DataFrame({
                'Parameter Name': 'Department',
                'Code values': unique_dept,
                'Decode value': unique_dept
            })
            
            # Extract unique CostCenterCode and CostCenterName
            unique_cct = param_df[['CostCenterCode', 'CostCenterName']].drop_duplicates()
            cct_params = pd.DataFrame({
                'Parameter Name': 'Costcenter',
                'Code values': unique_cct['CostCenterCode'],
                'Decode value': unique_cct['CostCenterName']
            })
            
            merge_df = pd.concat([role_params, dept_params, cct_params], ignore_index=True)
            
        except: 
            raise

        status = "succeed"
        self.logging[i].update({"data": merge_df.to_dict("list"), "status": status})
        logging.info(f"Collect param data, status: {status}")
