import glob
import logging
import re
import traceback
import pandas as pd
from .exception import CustomException
from .non_functional import CallFunction

class ModuleBOS(CallFunction):

    def __init__(self, params) -> None:
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
                data.append([element.strip('"') for element in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line.strip())])
            
            # verify data length 
            self.validate_row_length(data)
            
            # FILE: BOSTH 
            user_df = pd.DataFrame(data)
            user_df.columns = user_df.iloc[0].values
            user_df = user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            group_user_df = user_df.groupby(['employee_no','user_name','branch_code','employee_display_name'])['rolename']\
                .agg(lambda row: '+'.join(row.unique())).reset_index()
            group_user_df = group_user_df.replace(to_replace=r'NA\+|\+NA(?!;)', value='', regex=True)
            
            # FILE: BOSTH_Param
            param_df = self.collect_depend_file(i)
            group_param_df = param_df.groupby(['employee_no','username',])['rolename']\
                .agg(lambda row: '+'.join(row.unique())).reset_index()
            group_param_df = group_param_df.replace(to_replace=r'NA\+|\+NA(?!;)', value='', regex=True)
            
            # Merge 2 file BOSTH_Param / BOSTH
            group_merge_df = pd.merge(group_param_df, group_user_df, on='employee_no', how='right', suffixes=('_param', '_user'), validate='1:m')
            
            # Adjust column: rolename
            group_merge_df[['rolename_param', 'rolename_user']] = group_merge_df[['rolename_param', 'rolename_user']].fillna('NA')
            group_merge_df['rolename'] = group_merge_df[['rolename_param', 'rolename_user']].apply(lambda row: ';'.join(row), axis=1)
            group_merge_df = group_merge_df[['employee_no','user_name','employee_display_name','branch_code','rolename']]
            
            # Adjust column: user_name
            group_merge_df['user_name'] = group_merge_df['user_name'].apply(lambda row: row.replace('NTTHPDOM\\', '') if isinstance(row, str) else row)
            group_merge_df = group_merge_df[group_merge_df['user_name'] != '']
            
            # Adjust column: branch_code
            group_merge_df['branch_code'] = group_merge_df['branch_code'].astype(str).str.zfill(3)
            
            # Rename column
            group_merge_df = group_merge_df.rename(columns={
                'user_name' : 'AccountName',
                'employee_display_name' : 'AccountDescription',
                'branch_code' : 'AdditionalAttribute',
                'rolename' : 'EntitlementName'
            })
            
            # Mapping Data to Target Columns
            target_columns = self.logging[i]['columns']
            merge_df = pd.DataFrame(columns=target_columns)
            static_value = {
                'ApplicationCode' : 'BOS',
                'AccountType' : 'USR',
                'SecondEntitlementName' : 'NA',
                'ThirdEntitlementName' : 'NA',
                'AccountStatus' : 'A',
                'IsPrivileged' : 'N',
                'CreateDate' : 'NA',
                'LastLogin' : 'NA',
                'LastUpdatedDate' : 'NA',
                'Country' : 'TH',
            }
            final_bos = pd.concat([group_merge_df, merge_df],ignore_index=True)
            final_bos['AccountOwner'] = final_bos['AccountName']
            final_bos = final_bos.fillna(static_value)
            final_bos = final_bos.drop(columns='employee_no')
            final_bos = final_bos[target_columns].sort_values(by='AccountOwner',ignore_index=True)
            
        except Exception as err:
            # Extract the traceback to get error details
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)

        status = 'succeed'
        self.logging[i].update({'data': final_bos.to_dict('list'), 'status': status})
        logging.info(f'Collect user data, status: {status}')
    
    def collect_param_file(self, i: int, format_file: any) -> dict:
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})
        
        try:
            data = []
            for line in format_file:
                data.append([element.strip('"') for element in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line.strip())])
            
            # verify data length 
            self.validate_row_length(data)
            
            # FILE: BOSTH
            user_df = pd.DataFrame(data)
            user_df.columns = user_df.iloc[0].values
            user_df = user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            user_df['branch_code'] = user_df['branch_code'].astype(str).str.zfill(3)
            
            # FILE: BOSTH_Param
            param_df = self.collect_depend_file(i)
            
            # Mapping Data to Target Columns
            target_columns = self.logging[i]['columns']
            sec_param_list = pd.DataFrame(columns=target_columns)
            sec_parm_uni = param_df['rolename'].unique()
            sec_param_list['Code values'] = sec_parm_uni
            sec_param_list['Decode value'] = sec_parm_uni
            sec_param_list['Parameter Name'] = 'Security roles'
            
            app_param_list = pd.DataFrame(columns=target_columns)
            app_param_uni = user_df['rolename'].unique()
            app_param_list['Code values'] = app_param_uni
            app_param_list['Decode value'] = app_param_uni
            app_param_list['Parameter Name'] = 'Application roles'
            
            dept_param_list = user_df.iloc[:,[1,2]]
            dept_param_list = dept_param_list.drop_duplicates()
            dept_param_list.insert(0,'Parameter Name','Department code')
            dept_param_list = dept_param_list.rename(columns={
                'branch_code' : 'Code values',
                'branch_name' : 'Decode value'
            })
            # Replace value 'ศูนย์เงินสด' with 'Cash Hub'
            dept_param_list.loc[dept_param_list['Decode value'] == 'ศูนย์เงินสด', 'Decode value'] = 'Cash Hub'
            
            merge_df = pd.concat([sec_param_list,app_param_list],ignore_index=True)
            merge_df = pd.concat([merge_df, dept_param_list],ignore_index=True)
            
        except Exception as err:
            # Extract the traceback to get error details
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)
        
        status = 'succeed'
        self.logging[i].update({'data': merge_df.to_dict('list'), 'status': status})
        logging.info(f'Collect param data, status: {status}')
        
            
    def validate_row_length(self, rows_list: list[list], expected_length: int=6) -> None:
        # Assert that the length of the row matches the expected length
        errors = []
        for i, rows in enumerate(rows_list, 1):
            try:
                assert len(rows) == expected_length, f"Row {i} has data invalid. value:{rows}"
            except AssertionError as err:
                errors.append(str(err))
                
        if errors:
            raise Exception("\n".join(errors))
        
    def collect_depend_file(self, i: int) -> pd.DataFrame:
        
        logging.info('Lookup depend file')
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_depend_file', 'status': status})
        
        for full_depend in self.logging[i]['full_depend']:
            
            data = []
            if glob.glob(full_depend, recursive=True):
                format_file = self.read_file(i, full_depend)
                
                for line in format_file:
                    data.append([element.strip('"') for element in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line.strip())])
                
                # verify data length 
                self.validate_row_length(data)
            else:
                self.logging[i].update({'err': f'[File not found] at {full_depend}'})
                
        if 'err' in self.logging[i]:
            raise CustomException(err=self.logging)
        
        try:
            # Creating DataFrame
            param_df = pd.DataFrame(data)
            param_df.columns = param_df.iloc[0].values
            param_df = param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'status': status})
        
        return param_df