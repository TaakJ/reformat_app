import re
import pandas as pd
import logging
from .non_functional import CallFunction
from .exception import CustomException


class ModuleLMT(CallFunction):

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

    def validate_row_length(self, rows_list: list[list], expected_length: int = 7) -> None:
        errors = []
        for i, rows in enumerate(rows_list, 1):
            try:
                assert (len(rows) == expected_length or len(rows) == 1), f"row {i} does not values {expected_length} elements {rows}"
            except AssertionError as err:
                errors.append(str(err))

        if errors:
            raise Exception("Data issue: " + "\n".join(errors))

    def collect_user_file(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({"function": "collect_user_file", "status": status})

        try:            
            data = []
            for line in format_file:
                data.append([element.strip('"') for element in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line.strip())])
            self.validate_row_length(data)

            # Creating DataFrame
            user_df = pd.DataFrame(data)
            user_df.columns = user_df.iloc[0].values
            user_df = (user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True))
            user_df = user_df.map(lambda row: ("NA" if isinstance(row, str) and (row.lower() == "null" or row == "") else row))

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
            columns = self.logging[i]["columns"]
            merge_df = pd.DataFrame(columns=columns)
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
            final_lmt = final_lmt[columns]
            
        except:
            raise

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
            set_value = [
                {
                    "Parameter Name": "Security Roles",
                    "Code values": param_df["SecurityRoles"].unique(),
                    "Decode value": param_df["SecurityRoles"].unique(),
                },
                {
                    "Parameter Name": "Application Roles",
                    "Code values": param_df["ApplicationRoles"].unique(),
                    "Decode value": param_df["ApplicationRoles"].unique(),
                },
                {
                    "Parameter Name": "Program Template",
                    "Code values": param_df["ProgramTemplate"].unique(),
                    "Decode value": param_df["ProgramTemplate"].unique(),
                },
                {
                    "Parameter Name": "Department",
                    "Code values": param_df["Department"].unique(),
                    "Decode value": param_df["Department"].unique(),
                },
            ]
            merge_df = pd.DataFrame(set_value)
            merge_df = merge_df.explode(["Code values", "Decode value"]).reset_index(drop=True)

        except:
            raise

        status = "succeed"
        self.logging[i].update({"data": merge_df.to_dict("list"), "status": status})
        logging.info(f"Collect user param, status: {status}")
