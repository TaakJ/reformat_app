import logging
import re
import traceback
import pandas as pd
from .exception import CustomException
from .non_functional import CallFunction

class ModuleMOC(CallFunction):

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
                
        logging.info(f"Stop Run Module '{self.module}'\r\n")
        
    def validate_row_length(self, rows_list: list[list], expected_length: int=15) -> None:
        
        errors = []
        for i, rows in enumerate(rows_list, 1):
            try:
                # Assert that the length of the row matches the expected length
                assert (len(rows) == expected_length), f"Row {i} has data invalid. {rows}"
                
            except AssertionError as err:
                errors.append(str(err))
                
        if errors:
            raise Exception("\n".join(errors))

    def collect_user_file(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({"function": "collect_user_file", "status": status})

        try:
            data = []
            for line in format_file:
                data.append([element.strip('"') for element in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line.strip())])
            self.validate_row_length(data)

            # Creating DataFrame
            columns = self.logging[i]["columns"]
            user_df = pd.DataFrame(data, columns=columns)
            user_df = (user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True))

            # Replacing ‘null’ or Empty Strings with ‘NA’
            user_df = user_df.map(lambda row: ("NA" if isinstance(row, str) and (row.lower() == "null" or row == "") else row))

        except Exception as err:
            # Extract details from the traceback
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)

        status = "succeed"
        self.logging[i].update({"data": user_df.to_dict("list"), "status": status})
        logging.info(f"Collect user data, status: {status}")

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({"function": "collect_param_file", "status": status})

        try:
            data = []
            for line in format_file:
                data.append([element.strip('"') for element in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line.strip())])
            self.validate_row_length(data, 3)

            # Creating DataFrame
            target_columns = self.logging[i]["columns"]
            param_df = pd.DataFrame(data, columns=target_columns)
            param_df = (param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True))
            param_df = param_df.map(lambda row: ("NA" if isinstance(row, str) and (row.lower() == "null" or row == "") else row))

        except Exception as err:
            ## Extract the traceback to get error details
            error_frame = traceback.extract_tb(err.__traceback__)[-1]
            _, line_no, function, _ = error_frame
            err_msg = f"[Data issue] {str(err)}, found at function:{function}, line:{line_no}"
            raise Exception(err_msg)

        status = "succeed"
        self.logging[i].update({"data": param_df.to_dict("list"), "status": status})
        logging.info(f"Collect param data, status: {status}")
