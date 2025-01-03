import logging
import re
import traceback
import pandas as pd
from .exception import CustomException
from .non_functional import CallFunction

class ModuleLMT(CallFunction):

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
            data = []
            for line in format_file:
                data.append([element.strip('"') for element in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line.strip())])
                
            # verify data length 
            self.validate_row_length(data)

            # Creating DataFrame
            user_df = pd.DataFrame(data)
            user_df.columns = user_df.iloc[0].values
            user_df = (user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True))
            user_df = user_df.map(lambda row: ("NA" if isinstance(row, str) and (row.lower() == "null" or row == "") else row))

            # Adjust column: Department
            user_df['Department'] = user_df['Department'].apply(lambda row: ' '.join(row.replace('.', ' ').replace(',', ' ').split()).strip())
            
            # Adjust column: SecurityRoles, ApplicationRoles, ProgramTemplate
            user_df.loc[:, ["SecurityRoles", "ApplicationRoles", "ProgramTemplate"]] = (user_df.loc[:, ["SecurityRoles", "ApplicationRoles", "ProgramTemplate"]].fillna("NA"))
            user_df = user_df.drop_duplicates().reset_index(drop=True)
            
            # Group by specified columns and aggregate
            group_user_df = (user_df.groupby(["DisplayName", "EmployeeNo", "Username", "Department"]).agg(lambda row: "+".join(map(str, sorted(set(row))))).reset_index())
            
            # Adjust column: Username
            group_user_df["Username"] = group_user_df["Username"].apply(lambda row: (row.replace("NTTHPDOM\\", "") if isinstance(row, str) else row))
            
            # Adjust column: SecurityRoles, ApplicationRoles, ProgramTemplate
            group_user_df["Roles"] = group_user_df[["SecurityRoles", "ApplicationRoles", "ProgramTemplate"]].apply(lambda row: ";".join(filter(pd.notna, map(str, row))), axis=1)
            group_user_df["Roles"] = group_user_df["Roles"].replace(to_replace=r"NA\+|\+NA(?!;)", value="", regex=True)
            group_user_df = group_user_df.drop(group_user_df.loc[:, ["SecurityRoles", "ApplicationRoles", "ProgramTemplate"]],axis=1,)
            
            # Rename columns
            group_user_df = group_user_df.rename(columns={
                    "Username": "AccountOwner",
                    "Roles": "EntitlementName",
                    "DisplayName": "AccountDescription",
                    "Department": "AdditionalAttribute",
                }
            )
            
            # Mapping Data to Target Columns
            target_columns = self.logging[i]["columns"]
            merge_df = pd.DataFrame(columns=target_columns)
            static_values = {
                "ApplicationCode": "LMT",
                "AccountType": "USR",
                "SecondEntitlementName": "NA",
                "ThirdEntitlementName": "NA",
                "AccountStatus": "A",
                "IsPrivileged": "N",
                "CreateDate": "NA",
                "LastLogin": "NA",
                "LastUpdatedDate": "NA",
                "Country": "TH",
            }
            
            final_lmt = pd.merge(group_user_df, merge_df, on=["AccountOwner","EntitlementName","AccountDescription","AdditionalAttribute",],how="left", validate='1:1')
            final_lmt["AccountName"] = final_lmt["AccountOwner"]
            final_lmt = final_lmt.fillna(static_values)
            final_lmt = final_lmt[target_columns]
            
        except Exception as err:
            # Extract the traceback to get error details
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)

        status = "succeed"
        self.logging[i].update({"data": final_lmt.to_dict("list"), "status": status})
        logging.info(f"Collect user data, status: {status}")

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({"function": "collect_param_file", "status": status})

        try:
            data = []
            for line in format_file:
                data.append([element.strip('"') for element in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line.strip())])
            self.validate_row_length(data)

            # Creating DataFrame
            param_df = pd.DataFrame(data)
            param_df.columns = param_df.iloc[0].values
            param_df = (param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True))
            param_df = param_df.map(lambda row: ("NA" if isinstance(row, str) and (row.lower() == "null" or row == "") else row))
            
            # Mapping Data to Target Columns
            target_columns = self.logging[i]["columns"]
            
            # Extract unique SecurityRoles
            unique_sec = param_df['SecurityRoles'].unique()
            sec_params = pd.DataFrame([
                ['Security Roles', sec, sec] for sec in unique_sec
            ], columns=target_columns)
                        
            # Extract unique ApplicationRoles
            unique_app = param_df['ApplicationRoles'].unique()
            app_params = pd.DataFrame([
                ['Application Roles', app, app] for app in unique_app
            ], columns=target_columns)
            
            # Extract unique ProgramTemplate
            unique_temp = param_df['ProgramTemplate'].unique()
            temp_params = pd.DataFrame([
                ['Program Template', temp, temp] for temp in unique_temp
            ], columns=target_columns)
            
            # Extract unique Department
            unique_dept = param_df['Department'].apply(lambda row: ' '.join(row.replace('.', ' ').replace(',', ' ').split()).strip()).unique()
            dept_params = pd.DataFrame([
                ['Department', dept, dept] for dept in unique_dept
            ], columns=target_columns)
            
            merge_df = pd.concat([sec_params, app_params, temp_params, dept_params], ignore_index=True)
            
        except Exception as err:
            # Extract the traceback to get error details
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)

        status = "succeed"
        self.logging[i].update({"data": merge_df.to_dict("list"), "status": status})
        logging.info(f"Collect user param, status: {status}")


    def validate_row_length(self, rows_list: list[list], expected_length: int=7) -> None:
        # Assert that the length of the row matches the expected length
        errors = []
        for i, rows in enumerate(rows_list, 1):
            try:
                assert (len(rows) == expected_length or len(rows) == 1), f"Row {i} has data invalid. value:{rows}"
            except AssertionError as err:
                errors.append(str(err))
                
        if errors:
            raise Exception("\n".join(errors))