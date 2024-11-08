import re
import logging
from .non_functional import CallFunction
from .exception import CustomException
import pandas as pd


class ModuleIIC(CallFunction):

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

    def validate_row_length(self, rows_list: list[list], expected_length: int = 15) -> None:
        errors = []
        for i, rows in enumerate(rows_list, 1):
            try:
                assert (len(rows) == expected_length), f"row {i} does not have {expected_length} values {rows}"
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
            columns = self.logging[i]["columns"]
            user_df = pd.DataFrame(data, columns=columns)
            user_df = (user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True))

            # Replacing ‘null’ or Empty Strings with ‘NA’
            user_df = user_df.map(lambda row: ("NA" if isinstance(row, str) and (row.lower() == "null" or row == "") else row))

        except:
            raise

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
            columns = self.logging[i]["columns"]
            param_df = pd.DataFrame(data, columns=columns)
            param_df = (param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True))

            # Replacing ‘null’ or Empty Strings with ‘NA’
            param_df = param_df.map(lambda row: ("NA" if isinstance(row, str) and (row.lower() == "null" or row == "") else row))

        except:
            raise

        status = "succeed"
        self.logging[i].update({"data": param_df.to_dict("list"), "status": status})
        logging.info(f"Collect user param, status: {status}")
