import re
import pandas as pd
import logging
from .non_functional import CallFunction
from .exception import CustomException
# import traceback

class ModuleLDS(CallFunction):

    def __init__(self, params: any) -> None:
        for key, value in vars(params).items():
            setattr(self, key, value)

    def logSetter(self, log: list) -> None:
        self._log = log

    async def run_process(self) -> dict:
        
        logging.getLogger(__name__)
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

    def validate_row_length(self, rows_list: list[list], valid_lengths: list[int] = [32, 33]) -> None:
        errors = []
        for i, rows in enumerate(rows_list, 2):
            try:
                assert (len(rows) in valid_lengths), f"Row {i} has data invalid. {rows}"
            except AssertionError as err:
                errors.append(str(err))
        if errors:
            raise Exception("Data issue: " + "\n".join(errors))

    def read_format_file(self, format_file) -> list:
        
        def clean_and_split_line(line):
            # Remove newlines and whitespace, handle quoted strings
            cleaned_line = re.sub(r'\n\s*\w+', '', line).strip()
            cleaned_line = re.sub(r"\s+", " ", cleaned_line)
            split_line = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', cleaned_line, flags=re.DOTALL)
            return [x.strip() for x in split_line]
    
        clean_data = []
        for line in format_file:
            if re.match(r"\w+", line.strip()):
                split_line = clean_and_split_line(line)
                
                if len(split_line) >= 31:    
                    cleaned_row = []
                    for i, value in enumerate(split_line):
                        if i == 0:
                            split_values = re.split(r"\s+", value, maxsplit=1)
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
            
            # Pad rows with None values to ensure each row has 33 elements
            clean_data = [row + [None] * (33 - len(row)) for row in clean_data]
            
            # Creating DataFrame
            columns = ["Rownum", "UserID", "UserName", "FullName", "Email", "Approve_Limit", "User_Active", "User_status", "RoleID", "RoleName", "RLOC", "WLOC", 
            "Role_Active", "Role_status", "CostCenterCode", "CostCenterName", "BranchCode", "CostCenterNameThai", "Costcenter_Effect", "Costcenter_Expire",
            "Costcenter_Active", "Costcenter_status", "SectorID", "SectorName", "Sector_Effect", "Sector_Expire", "Sector_Active", "Sector_status", 
            "LastLogin_Date", "edit_name", "edit_date", "Remark","Exceed_column"]
            user_df = pd.DataFrame(clean_data, columns=columns)
            user_df = user_df.iloc[:, :-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
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
            
            # Pad rows with None values to ensure each row has 33 elements
            clean_data = [row + [None] * (33 - len(row)) for row in clean_data]
            
            # Creating DataFrame
            columns = ["Rownum", "UserID", "UserName", "FullName", "Email", "Approve_Limit", "User_Active", "User_status", "RoleID", "RoleName", "RLOC", "WLOC", 
            "Role_Active", "Role_status", "CostCenterCode", "CostCenterName", "BranchCode", "CostCenterNameThai", "Costcenter_Effect", "Costcenter_Expire",
            "Costcenter_Active", "Costcenter_status", "SectorID", "SectorName", "Sector_Effect", "Sector_Expire", "Sector_Active", "Sector_status", 
            "LastLogin_Date", "edit_name", "edit_date", "Remark", "Exceed"]
            param_df = pd.DataFrame(clean_data, columns=columns)
            param_df = param_df.iloc[:, :-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            param_df = param_df.map(lambda row: ("NA" if isinstance(row, str) and (row.strip().lower() == "null" or row.strip() == "") else row))
            
            # Adjust column: CostCenterName
            param_df['CostCenterName'] = param_df['CostCenterName'].apply(lambda row: ' '.join(row.replace('.', ' ').replace(',', ' ').split()).strip())
            
            # Mapping Data to Target Columns
            target_columns = self.logging[i]["columns"]
            
            # Extract unique RoleID and RoleName
            unique_roles = param_df[['RoleID', 'RoleName']].drop_duplicates()
            role_params = pd.DataFrame([
                ['Role', role_id, role_name] for role_id, role_name in zip(unique_roles['RoleID'], unique_roles['RoleName'])
            ], columns=target_columns)
            
            # Extract unique CostCenterName
            unique_dept = param_df['CostCenterName'].unique()
            dept_params = pd.DataFrame([
                ['Department', dept, dept] for dept in unique_dept
            ], columns=target_columns)
            
            # Extract unique CostCenterCode and CostCenterName
            unique_cct = param_df[['CostCenterCode', 'CostCenterName']].drop_duplicates()
            cct_params = pd.DataFrame([
                ['Costcenter', code, name] for code, name in zip(unique_cct['CostCenterCode'], unique_cct['CostCenterName'])
            ], columns=target_columns)
                        
            merge_df = pd.concat([role_params, dept_params, cct_params], ignore_index=True)
            
        except:
            raise

        status = "succeed"
        self.logging[i].update({"data": merge_df.to_dict("list"), "status": status})
        logging.info(f"Collect param data, status: {status}")
