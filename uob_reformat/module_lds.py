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

        result = {'module': self.module, 'task': 'Completed'}
        try:
            self.colloct_setup()
            self.clear_target_file()
            
            await self.check_source_file()
            await self.separate_data_file()
            if self.store_tmp is True:
                await self.genarate_tmp_file()
            await self.genarate_target_file()

        except CustomException as err:
            logging.error('See Error Details: log_error.log')
            
            logger = err.setup_errorlog(log_name=__name__)
            while True:
                try:
                    logger.error(next(err))
                except StopIteration:
                    break

            result.update({'task': 'Uncompleted'})

        logging.info(f"Stop Run Module '{self.module}'\r\n")
        
        return result
    
    def parse_datetime(self, date_str):
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', 
                    '%Y-%m-%d %H:%M:%S',
                    '%d/%m/%Y %H:%M:%S.%f', 
                    '%d/%m/%Y %H:%M:%S',
                    '%Y-%m-%d',
                    '%d-%m-%Y',
                    '%Y/%m/%d',
                    '%d/%m/%Y',
                    '%m/%d/%Y'):
            try:
                return pd.to_datetime(date_str, format=fmt)
            except ValueError:
                continue
        return pd.NaT
    
    def validate_row_length(self, rows_list: list[list], valid_lengths: list[int] = [3,32,33]) -> None:
        errors = []
        for i, rows in enumerate(rows_list, 2):
            try:
                assert len(rows) in valid_lengths, f"Row {i} does not match elements: {rows}"
            except AssertionError as err:
                errors.append(str(err))
                    
        if errors:
            raise Exception("Column not match" + "\n".join(errors))
    
    def read_format_file(self, format_file) -> list:
        
        data = [re.sub(r'(?<!\.),', '||', ''.join(re.findall(r'\w+.*', line.strip()))).split('||') for line in format_file if re.findall(r'\w+.*', line.strip())]
        
        clean_data = []
        for rows, _data in enumerate(data):
            if rows == 0:
                clean_data += [re.sub(r'\s+', ',', ','.join(_data)).split(',')]
            else:
                fix_value = []
                for idx, value in enumerate(_data, 1):
                    if idx == 1:
                        value = re.sub(r'\s+', ',', value).split(',')
                        fix_value.extend(value)
                    else:
                        fix_value.append(value)
                clean_data.append(fix_value)
                
        return clean_data

    def collect_user_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_user_file', 'status': status})
        
        try:
            clean_data = self.read_format_file(format_file)
            self.validate_row_length(clean_data)
            
            # Creating DataFrame
            user_df = pd.DataFrame(clean_data)
            user_df.columns = user_df.iloc[0].values
            user_df = user_df.iloc[1:-1, :-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            # Replacing ‘null’ or Empty Strings with ‘NA’
            user_df = user_df.map(lambda row: 'NA' if isinstance(row, str) and (row.lower() == 'null' or row == '') else row)
            
            # Mapping Data to Target Columns
            set_value = dict.fromkeys(self.logging[i]['columns'], 'NA')
            set_value.update(
                {
                    'ApplicationCode': 'LDS', 
                    'AccountOwner': user_df['UserName'], 
                    'AccountName': user_df['UserName'],
                    'AccountType': 'USR',
                    'EntitlementName': user_df['RoleID'],
                    'AccountStatus': user_df['User_Active'].apply(lambda row: 'A' if row.lower() == 'active' else 'D'),
                    'IsPrivileged': 'N',
                    'AccountDescription': user_df['FullName'],
                    'LastLogin':  user_df['LastLogin_Date'].apply(self.parse_datetime).dt.strftime('%Y%m%d%H%M%S'),
                    'LastUpdatedDate': user_df['edit_date'].apply(self.parse_datetime).dt.strftime('%Y%m%d%H%M%S'),
                    'AdditionalAttribute': user_df[['CostCenterName','CostCenterCode']].apply(lambda row: '#'.join(row), axis=1),
                    'Country': "TH"
                }
            )
            user_df = user_df.assign(**set_value)
            user_df = user_df.drop(user_df.iloc[:,:32].columns, axis=1)
            
        except:
            raise
        
        status = 'succeed'
        self.logging[i].update({'data': user_df.to_dict('list'), 'status': status})
        logging.info(f'Collect user data, status: {status}')
        
    def collect_param_file(self, i: int, format_file: any) -> dict:
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})
        
        try:
            clean_data = self.read_format_file(format_file)
            self.validate_row_length(clean_data)
            
            # Creating DataFrame
            param_df = pd.DataFrame(clean_data)
            param_df.columns = param_df.iloc[0].values
            param_df = param_df.iloc[1:-1, :-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            # Replacing ‘null’ or Empty Strings with ‘NA’
            param_df = param_df.map(lambda row: 'NA' if isinstance(row, str) and (row.strip().lower() == 'null' or row.strip() == '') else row)
            
            # Mapping Data to Target Columns
            set_value = [
                {
                    'Parameter Name': 'Role',
                    'Code values': param_df['RoleID'].unique(),
                    'Decode value': param_df['RoleName'].unique(),
                },
                {
                    'Parameter Name': 'Department',
                    'Code values': param_df['CostCenterName'].unique(),
                    'Decode value': param_df['CostCenterName'].unique(),
                },
                {
                    'Parameter Name': 'Costcenter',
                    'Code values': param_df['CostCenterCode'].unique(),
                    'Decode value': param_df['CostCenterName'].unique(),
                },
            ]
            param_df = pd.DataFrame(set_value)
            param_df = param_df.explode(['Code values', 'Decode value']).reset_index(drop=True)

        except:
            raise

        status = 'succeed'
        self.logging[i].update({'data': param_df.to_dict('list'), 'status': status})
        logging.info(f'Collect param data, status: {status}')