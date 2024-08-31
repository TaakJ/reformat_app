import pandas as pd
import logging
from .non_functional import CallFunction
from .exception import CustomException

class ModuleCUM(CallFunction):

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

            if self.backup is True:
                self.achieve_backup()

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
                    logger.exception(next(err))
                except StopIteration:
                    break

            result.update({'task': 'Uncompleted'})

        logging.info(f"Stop Run Module '{self.module}'\r\n")

        return result
    
    def read_format_file(self, format_file) -> list:
        
        data = []
        sheet_list = [sheet for sheet in format_file.sheet_names()]
        for sheets in sheet_list:
            cells = format_file.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                by_sheets = [str(cells.cell(row, col).value).strip() for col in range(cells.ncols)][1:]
                if not all(empty == '' for empty in by_sheets):
                    data.append(by_sheets)
                    
        return data

    def collect_user_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_user_file', 'status': status})
        
        try:
            
            clean_data = self.read_format_file(format_file)
            
            ## set dataframe  
            user_df = pd.DataFrame(clean_data)
            user_df.columns = user_df.iloc[0].values
            user_df =  user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            # group by column
            user_df = user_df.groupby('USER_ID', sort=False)
            user_df = user_df.agg(lambda row: '+'.join(row.unique())).reset_index()
            
            ## mapping data to column
            set_value = dict.fromkeys(self.logging[i]['columns'], 'NA')
            set_value.update(
                {
                    'ApplicationCode': 'CUM', 
                    'AccountOwner': user_df['USER_ID'], 
                    'AccountName': user_df['USER_ID'],
                    'AccountType': 'USR',
                    'EntitlementName': user_df['GROUP_NO'],
                    'AccountStatus': 'A',
                    'IsPrivileged': 'N',
                    'AccountDescription': user_df[['NAME','SURNAME']].apply(lambda row: row['NAME'] + ' ' + row['SURNAME'], axis=1),
                    'CreateDate': pd.to_datetime(user_df['VALID_FROM'], dayfirst=True).dt.strftime('%Y%m%d%H%M%S'), 
                    'LastLogin': pd.to_datetime(user_df['Last Usage'], dayfirst=True).dt.strftime('%Y%m%d%H%M%S'),
                    'AdditionalAttribute': user_df['DEPARTMENT'],
                    'Country': 'TH'
                }
            )
            user_df = user_df.assign(**set_value)
            user_df = user_df.drop(user_df.iloc[:,:14].columns, axis=1)
            
        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': user_df.to_dict('list'), 'status': status})
        logging.info(f"Collect user data, status: {status}")

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})
        
        try:
            
            clean_data = self.read_format_file(format_file)
            
            ## set dataframe  
            param_df = pd.DataFrame(clean_data)
            param_df.columns = param_df.iloc[0].values
            param_df =  param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## mapping data to column
            set_value = [
                {
                    'Parameter Name': 'Group_No',
                    'Code value': param_df['GROUP_NO'].unique(),
                    'Decode value': param_df['GROUP_NO'].unique(),
                },
                {
                    'Parameter Name': 'Department',
                    'Code value': param_df['DEPARTMENT'].unique(),
                    'Decode value': param_df['DEPARTMENT'].unique(),
                },
            ]
            param_df = pd.DataFrame(set_value)
            param_df = param_df.explode(['Code value', 'Decode value']).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'data': param_df.to_dict('list'), 'status': status})
        logging.info(f'Collect param data, status: {status}')
