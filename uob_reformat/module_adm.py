import logging
import re
import traceback
import pandas as pd
from .exception import CustomException
from .non_functional import CallFunction

class ModuleADM(CallFunction):

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

        status = 'failed'
        self.logging[i].update({'function': 'collect_user_file', 'status': status})

        try:
            data = []
            for line in format_file:
                data.append([element.strip('"') for element in re.split(r'\|\|(?=(?:[^"]*"[^"]*")*[^"]*$)', line.strip())])
            
            # Pad rows with None values to ensure each row has 8 elements
            clean_data = [row + [None] * (8 - len(row)) for row in data]
            
            # verify data length 
            self.validate_row_length(clean_data)
            
            # Creating DataFrame
            columns = ['User-ID','User Full Name','Department code','Employee ID','Group','Zone','Role', 'Date']
            user_df = pd.DataFrame(clean_data, columns=columns)
            user_df = user_df.apply(lambda row: row.str.strip()).reset_index(drop=True)
            user_df = user_df.map(lambda row: 'NA' if isinstance(row, str) and (row.lower() == 'null' or row == '') else row)
            
            # Adjust column: Group
            user_df['Group'] = user_df['Group'].apply(lambda row: ' '.join(row.replace('.', ' ').replace(',', ' ').split()).strip())
            
            # Group by specified columns and aggregate
            user_df = user_df.groupby('User-ID', sort=False).agg(lambda row: '+'.join(filter(pd.notna, row.unique()))).reset_index()
            user_df = user_df.replace(to_replace=r'NA\+|\+NA(?!;)', value='', regex=True)
            
            # Mapping Data to Target Columns
            target_columns  = self.logging[i]["columns"]
            mapping = dict.fromkeys(target_columns , "NA")
            mapping.update(
                {
                    'ApplicationCode': 'ADM',
                    'AccountOwner': user_df['User-ID'],
                    'AccountName': user_df['User-ID'],
                    'AccountType': 'USR',
                    'EntitlementName': user_df['Group'],
                    'SecondEntitlementName': user_df['Role'],
                    'ThirdEntitlementName': user_df['Zone'],
                    'AccountStatus': 'A',
                    'IsPrivileged': 'N',
                    'AccountDescription': user_df['User Full Name'],
                    'AdditionalAttribute': user_df['Department code'],
                    'Country': 'TH',
                }
            )
            user_df = user_df.assign(**mapping)
            user_df = user_df.drop(user_df.iloc[:, :8].columns, axis=1)
            
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

        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})

        try:
            data = []
            for line in format_file:
                data.append([element.strip('"') for element in re.split(r'\|\|(?=(?:[^"]*"[^"]*")*[^"]*$)', line.strip())])
            
            # Pad rows with None values to ensure each row has 8 elements
            clean_data = [row + [None] * (8 - len(row)) for row in data]
            
            # verify data length 
            self.validate_row_length(clean_data)
            
            # Creating DataFrame
            columns = ['User-ID','User Full Name','Department code','Employee ID','Group','Zone','Role', 'Date']
            param_df = pd.DataFrame(clean_data, columns=columns)
            param_df = param_df.apply(lambda row: row.str.strip()).reset_index(drop=True)
            param_df = param_df.map(lambda row: 'NA' if isinstance(row, str) and (row.lower() == 'null' or row == '') else row)
            
            # Mapping Data to Target Columns
            target_columns = self.logging[i]["columns"]
            
            # Extract unique Group
            unique_group = param_df['Group'].apply(lambda row: ' '.join(row.replace('.', ' ').replace(',', ' ').split()).strip()).unique()
            group_params = pd.DataFrame([
                ['GroupDetail', group, group] for group in unique_group
            ], columns=target_columns)

            # Extract unique Role
            unique_role = param_df['Role'].unique()
            role_params = pd.DataFrame([
                ['RoleDetail', role, role] for role in unique_role
            ], columns=target_columns)
            
            # Extract unique Zone
            unique_zone = param_df['Zone'].unique()
            zone_params = pd.DataFrame([
                ['Zone', zone, zone] for zone in unique_zone
            ], columns=target_columns)
            
            # Extract unique Department code
            unique_dept = param_df['Department code'].unique()
            dept_params = pd.DataFrame([
                ['Department', dept, dept] for dept in unique_dept
            ], columns=target_columns)
            
            merge_df = pd.concat([group_params, role_params, zone_params, dept_params], ignore_index=True)
            
        except Exception as err:
            # Extract the traceback to get error details
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)

        status = "succeed"
        self.logging[i].update({"data": merge_df.to_dict("list"), "status": status})
        logging.info(f"Collect user param, status: {status}")
        
        
    def validate_row_length(self, rows_list: list[list], expected_length: int=8) -> None:
        # Assert that the length of the row matches the expected length
        errors = []
        for i, rows in enumerate(rows_list, 1):
            try:
                assert len(rows) == expected_length, f"Row {i} has data invalid. value:{rows}"
            except AssertionError as err:
                errors.append(str(err))
                
        if errors:
            raise Exception("\n".join(errors))
