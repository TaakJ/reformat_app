import glob
import logging
import re
import traceback
from pathlib import Path
import pandas as pd
from .exception import CustomException
from .non_functional import CallFunction

class ModuleICA(CallFunction):

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

    def collect_user_file(self, i: int, format_file: any) -> str:

        status = "failed"
        self.logging[i].update({"function": "collect_user_file", "status": status})

        try:
            # FILE: ICAS_TBL_USER
            data = [re.sub(r'(?<!\.)\x07', '||', line.strip()).split('||') for line in format_file
                    if 'a.' not in line.lower() and 'pongchet' not in line.lower()]
            
            # verify data length 
            self.validate_row_length(data, [3, 5, 21])
            
            columns = ["Record_Type","USER_ID","LOGIN_NAME","FULL_NAME","PASSWORD","LOCKED_FLAG","FIRST_LOGIN_FLAG","LAST_ACTION_TYPE","CREATE_USER_ID","CREATE_DTM","LAST_UPDATE_USER_ID",
                    "LAST_UPDATE_DTM","LAST_LOGIN_ATTEMPT","ACCESS_ALL_BRANCH_FLAG","HOME_BRANCH","HOME_BANK","LOGIN_RETRY_COUNT","LAST_CHANGE_PASSWORD","DELETE_FLAG",
                    "LAST_LOGIN_SUCCESS","LAST_LOGIN_FAILED"]
            tbl_user_df = pd.DataFrame(data, columns=columns)
            tbl_user_df = (tbl_user_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True))

            # FILE: ICAS_TBL_USER_GROUP, ICAS_TBL_USER_BANK_BRANCH, ICAS_TBL_GROUP
            tbl_user_group_df, tbl_user_bank_df, _ = self.collect_depend_file(i)

            # Merge file: ICAS_TBL_USER_GROUP with ICAS_TBL_USER
            entitlement_name = pd.merge(tbl_user_df, tbl_user_group_df, on="USER_ID", how="left", validate="1:m")
            entitlement_name = entitlement_name.fillna("NA")
            entitlement_name_group = (entitlement_name.groupby("USER_ID")["GROUP_ID"].apply(lambda row: "+".join(map(str, sorted(set(row))))).reset_index())
            entitlement_name_group = entitlement_name_group.replace(to_replace=r"NA\+|\+NA(?!;)", value="", regex=True)

            # Merge file: ICAS_TBL_USER with ICAS_TBL_USER_GROUP
            result_ica = pd.merge(entitlement_name_group, tbl_user_df, on="USER_ID", how="left", validate="1:m")
            result_ica = result_ica[["USER_ID","LOGIN_NAME","GROUP_ID","LOCKED_FLAG","FULL_NAME","CREATE_DTM","LAST_LOGIN_ATTEMPT","LAST_UPDATE_DTM"]]

            # Merge file: ICAS_TBL_USER with ICAS_TBL_USER_BANK_BRANCH
            branch_code = pd.merge(tbl_user_df, tbl_user_bank_df, on="USER_ID", how="left", validate="1:m")
            branch_code = branch_code[["USER_ID", "HOME_BANK", "HOME_BRANCH", "BRANCH_CODE"]]
            branch_code["HOME_BANK"] = branch_code["HOME_BANK"].fillna("NA")
            branch_code["HOME_BRANCH"] = branch_code["HOME_BRANCH"].fillna("NA")
            branch_code["BRANCH_CODE"] = branch_code["BRANCH_CODE"].fillna("NA")
            branch_code["BANK+BRANCH"] = (branch_code[["HOME_BANK", "HOME_BRANCH", "BRANCH_CODE"]].astype(str).agg("#".join, axis=1))
            final_branch_code = branch_code[["USER_ID", "BANK+BRANCH"]]
            final_ica = pd.merge(result_ica, final_branch_code, on="USER_ID", how="left", validate='1:m')

            date_time_col = ["CREATE_DTM", "LAST_LOGIN_ATTEMPT", "LAST_UPDATE_DTM"]
            for col in date_time_col:
                final_ica[col] = final_ica[col].apply(self.parse_datetime).dt.strftime("%Y%m%d%H%M%S")

            final_ica["FULL_NAME"] = final_ica["FULL_NAME"].apply(self.clean_fullname)
            final_ica["LOCKED_FLAG"] = final_ica["LOCKED_FLAG"].apply(lambda row: "D" if row == "1" else "A")

            # Mapping Data to Target Columns
            target_columns = self.logging[i]["columns"]
            merge_df = pd.DataFrame(columns=target_columns)
            static_value = {
                "ApplicationCode": "ICA",
                "AccountType": "USR",
                "SecondEntitlementName": "NA",
                "ThirdEntitlementName": "NA",
                "IsPrivileged": "N",
                "Country": "TH",
            }

            column_mapping = {
                "LOGIN_NAME": "AccountOwner",
                "GROUP_ID": "EntitlementName",
                "LOCKED_FLAG": "AccountStatus",
                "FULL_NAME": "AccountDescription",
                "CREATE_DTM": "CreateDate",
                "LAST_LOGIN_ATTEMPT": "LastLogin",
                "LAST_UPDATE_DTM": "LastUpdatedDate",
                "BANK+BRANCH": "AdditionalAttribute",
            }
            final_ica = final_ica.rename(columns=column_mapping)
            merge_df = pd.concat([merge_df, final_ica], ignore_index=True)
            merge_df = merge_df.drop(columns="USER_ID")
            merge_df["AccountName"] = merge_df["AccountOwner"]
            merge_df = merge_df.fillna(static_value)

        except Exception as err:
            # Extract the traceback to get error details
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)

        status = "succeed"
        self.logging[i].update({"data": merge_df.to_dict("list"), "status": status})
        logging.info(f"Collect user data, status: {status}")

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({"function": "collect_param_file", "status": status})

        try:
            # FILE: ICAS_TBL_USER
            data = [re.sub(r"(?<!\.)\x07", "||", line.strip()).split("||") for line in format_file]
            
            # verify data length 
            self.validate_row_length(data, [3, 5, 21])
            
            columns = ["Record_Type","USER_ID","LOGIN_NAME","FULL_NAME","PASSWORD","LOCKED_FLAG","FIRST_LOGIN_FLAG","LAST_ACTION_TYPE","CREATE_USER_ID","CREATE_DTM","LAST_UPDATE_USER_ID",
                    "LAST_UPDATE_DTM","LAST_LOGIN_ATTEMPT","ACCESS_ALL_BRANCH_FLAG","HOME_BRANCH","HOME_BANK","LOGIN_RETRY_COUNT","LAST_CHANGE_PASSWORD","DELETE_FLAG",
                    "LAST_LOGIN_SUCCESS","LAST_LOGIN_FAILED"]
            tbl_user_df = pd.DataFrame(data, columns=columns)
            tbl_user_df = (tbl_user_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True))

            # FILE: ICAS_TBL_USER_GROUP, ICAS_TBL_USER_BANK_BRANCH, ICAS_TBL_GROUP
            _, tbl_user_bank_df, tbl_group_df = self.collect_depend_file(i)

            # Merge file: ICAS_TBL_USER with ICAS_TBL_USER_BANK_BRANCH
            target_columns = self.logging[i]["columns"]
            merge_df = pd.DataFrame(columns=target_columns)

            home_bank = {
                "Parameter Name": "HOME_BANK",
                "Code values": "024",
                "Decode value": "UOBT",
            }
            param_home_bank = pd.DataFrame([home_bank])

            # Adjusting Column: group_id
            param_group_unique = tbl_group_df["GROUP_ID"].unique()
            filter_param_group = tbl_group_df[tbl_group_df["GROUP_ID"].isin(param_group_unique)]
            filter_param_group = filter_param_group[["GROUP_ID", "GROUP_NAME"]]
            filter_param_group.insert(0, "Parameter Name", "User Group")
            filter_param_group.rename(columns={"GROUP_ID": "Code values", "GROUP_NAME": "Decode value"},inplace=True)
            merge_df = pd.concat([merge_df, filter_param_group], ignore_index=True)

            # Adjusting Column: home_bank
            merge_df = pd.concat([merge_df, param_home_bank], ignore_index=True)

            # Adjusting Column: home_branch
            param_home_branch_list = pd.DataFrame(columns=("Parameter Name", "Code values", "Decode value"))
            param_home_branch_uni = tbl_user_df["HOME_BRANCH"].unique()
            param_home_branch_list["Code values"] = param_home_branch_uni
            param_home_branch_list["Decode value"] = param_home_branch_uni
            param_home_branch_list["Parameter Name"] = "HOME_BRANCH"
            merge_df = pd.concat([merge_df, param_home_branch_list], ignore_index=True)

            # Adjusting Column: department
            param_dept_list = pd.DataFrame(columns=("Parameter Name", "Code values", "Decode value"))
            param_dept_uni = tbl_user_bank_df["BRANCH_CODE"].unique()
            param_dept_list["Code values"] = param_dept_uni
            param_dept_list["Decode value"] = param_dept_uni
            param_dept_list["Parameter Name"] = "Department"
            merge_df = pd.concat([merge_df, param_dept_list], ignore_index=True)

        except Exception as err:
            # Extract the traceback to get error details
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)

        status = "succeed"
        self.logging[i].update({"data": merge_df.to_dict("list"), "status": status})
        logging.info(f"Collect param data, status: {status}")
        
    def validate_row_length(self, rows_list: list[list], valid_lengths: list[int]) -> None:
        # Assert that the length of the row matches the expected length
        errors = []
        for i, rows in enumerate(rows_list):
            try:
                assert (len(rows) in valid_lengths), f"Row {i} has data invalid. value:{rows}"
            except AssertionError as err:
                errors.append(str(err))
        
        if errors:
            raise Exception("\n".join(errors))
        
    def collect_depend_file(self, i: int) -> pd.DataFrame:

        logging.info("Lookup depend file")

        tbl = {}
        for full_depend in self.logging[i]["full_depend"]:

            data = []
            if glob.glob(full_depend, recursive=True):

                format_file = self.read_file(i, full_depend)
                for line in format_file:
                    data += [re.sub(r"(?<!\.)\x07", "||", line.strip()).split("||")]

                tbl_name = Path(full_depend).name
                if tbl_name in tbl:
                    tbl[tbl_name].extend(data)
                else:
                    tbl[tbl_name] = data
            else:
                self.logging[i].update({"err": f"[File not found] at {full_depend}"})

            if "err" in self.logging[i]:
                raise CustomException(err=self.logging)

        status = "failed"
        self.logging[i].update({"function": "collect_depend_file", "status": status})

        try:
            # verify data length 
            self.validate_row_length(tbl["ICAS_TBL_USER_GROUP"], [3, 5, 7])
            
            # FILE: ICAS_TBL_USER_GROUP
            columns = ["Record_Type","GROUP_ID","USER_ID","CREATE_USER_ID","CREATE_DTM","LAST_UPDATE_USER_ID","LAST_UPDATE_DTM"]
            tbl_user_group_df = pd.DataFrame(tbl["ICAS_TBL_USER_GROUP"], columns=columns)
            tbl_user_group_df = (tbl_user_group_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True))

            # verify data length 
            self.validate_row_length(tbl["ICAS_TBL_USER_BANK_BRANCH"], [3, 5, 9])
            
            # FILE: ICAS_TBL_USER_BANK_BRANCH
            columns = ["Record_Type","USER_ID","BANK_CODE","BRANCH_CODE","SUB_SYSTEM_ID","ACCESS_ALL_BRANCH_IN_HUB","DEFAULT_BRANCH_FLAG","CREATE_USER_ID","CREATE_DTM"]
            tbl_user_bank_df = pd.DataFrame(tbl["ICAS_TBL_USER_BANK_BRANCH"], columns=columns)
            tbl_user_bank_df = (tbl_user_bank_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True))

            # verify data length 
            self.validate_row_length(tbl["ICAS_TBL_GROUP"], [3, 5, 14])
            
            # FILE: ICAS_TBL_GROUP
            columns = ["Record_Type","GROUP_ID","SUB_SYSTEM_ID","GROUP_NAME","RESTRICTION","ABLE_TO_REVERIFY_FLAG","DESCRIPTION","DEFAULT_FINAL_RESULT","DELETE_FLAG","CREATE_USER_ID",
                    "CREATE_DTM","LAST_UPDATE_USER_ID","LAST_UPDATE_DTM","DELETE_DTM"]
            tbl_group_df = pd.DataFrame(tbl["ICAS_TBL_GROUP"], columns=columns)
            tbl_group_df = (tbl_group_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True))

        except:
            raise

        status = "succeed"
        self.logging[i].update({"status": status})

        return tbl_user_group_df, tbl_user_bank_df, tbl_group_df
    
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
    
    def clean_fullname(self, name):
        name = name.replace(",", "")
        name = re.sub(r"\d+", "", name)
        name = name.strip()
        return name
