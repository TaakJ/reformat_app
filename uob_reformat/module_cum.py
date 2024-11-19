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
            self.collect_setup()
            self.clear_target_file()

            await self.check_source_file()
            await self.separate_data_file()
            if self.store_tmp is True:
                await self.generate_tmp_file()
            await self.generate_target_file()

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
    
    def validate_row_length(self, rows_list: list[list], valid_lengths: list[int] = [1,14]) -> None:
        errors = []
        for i, rows in enumerate(rows_list, 2):
            try:
                assert len(rows) in valid_lengths, f"row {i} does not match values {rows}"
            except AssertionError as err:
                errors.append(str(err))
        if errors:
            raise Exception("Data issue: " + "\n".join(errors))
    
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
            self.validate_row_length(clean_data)
            
            # Creating DataFrame
            user_df = pd.DataFrame(clean_data)
            user_df.columns = user_df.iloc[0].values
            user_df =  user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            user_df = user_df.map(lambda row: 'NA' if isinstance(row, str) and (row.lower() == 'null' or row == '') else row)
            
            # Adjust column: DEPARTMENT
            user_df['DEPARTMENT'] = user_df['DEPARTMENT'].apply(lambda row: ' '.join(row.replace('.', ' ').replace(',', ' ').split()).strip())
            
            # Group by specified columns and aggregate
            user_df = user_df.groupby('USER_ID', sort=False).agg(lambda row: '+'.join(filter(pd.notna, row.unique()))).reset_index()
            user_df = user_df.replace(to_replace=r'NA\+|\+NA(?!;)', value='', regex=True)
            
            # Mapping Data to Target Columns
            target_columns  = self.logging[i]["columns"]
            mapping = dict.fromkeys(target_columns, "NA")
            mapping.update(
                {
                    'ApplicationCode': 'CUM', 
                    'AccountOwner': user_df['USER_ID'], 
                    'AccountName': user_df['USER_ID'],
                    'AccountType': 'USR',
                    'EntitlementName': user_df['GROUP_NO'],
                    'AccountStatus': 'A',
                    'IsPrivileged': 'N',
                    'AccountDescription': user_df[['NAME','SURNAME']].apply(lambda row: row['NAME'] + ' ' + row['SURNAME'], axis=1),
                    'CreateDate': user_df['VALID_FROM'].apply(self.parse_datetime).dt.strftime('%Y%m%d%H%M%S'), 
                    'LastLogin': user_df['Last Usage'].apply(self.parse_datetime).dt.strftime('%Y%m%d%H%M%S'),
                    'AdditionalAttribute': user_df['DEPARTMENT'],
                    'Country': 'TH'
                }
            )
            user_df = user_df.assign(**mapping)
            user_df = user_df.drop(user_df.iloc[:,:14].columns, axis=1)
            
        except:
            raise

        status = 'succeed'
        self.logging[i].update({'data': user_df.to_dict('list'), 'status': status})
        logging.info(f"Collect user data, status: {status}")

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})
        
        try:
            clean_data = self.read_format_file(format_file)
            self.validate_row_length(clean_data)
            
            # Creating DataFrame
            param_df = pd.DataFrame(clean_data)
            param_df.columns = param_df.iloc[0].values
            param_df =  param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            param_df = param_df.map(lambda row: 'NA' if isinstance(row, str) and (row.lower() == 'null' or row == '') else row)
            
            # Mapping Data to Target Columns
            target_columns = self.logging[i]["columns"]
            
            # Extract unique Group
            unique_group = param_df['GROUP_NO'].unique()
            group_params = pd.DataFrame([
                ['Group_No', group, group] for group in unique_group
            ], columns=target_columns)

            # Extract unique Department
            unique_dept = param_df['DEPARTMENT'].apply(lambda row: ' '.join(row.replace('.', ' ').replace(',', ' ').split()).strip()).unique()
            dept_params = pd.DataFrame([
                ['Department', dept, dept] for dept in unique_dept
            ], columns=target_columns)
            
            merge_df = pd.concat([group_params, dept_params], ignore_index=True)
            
        except:
            raise
        
        status = "succeed"
        self.logging[i].update({"data": merge_df.to_dict("list"), "status": status})
        logging.info(f"Collect param data, status: {status}")
