from log import record_log
from exception import CustomException
from setup import setup_log, Folder, clear_tmp
from datetime import datetime
import pandas as pd
from os.path import join
import logging
from module import convert_2_files


class call_function(convert_2_files, record_log):
    pass

class module_adm(call_function):
    
    def _log_setter(self, log):
        self._log = log
    
    def _params(self, module, params):
        for key, value in params.items():
            setattr(self, key, value)
            
        self.date = datetime.now()
        self.module = module
        self.input_dir = [join(self.config[self.module]["input_dir"], self.config[self.module]["file"])]
        for m in self.config[self.module]["require"]:
            self.input_dir += [join(self.config[m]["input_dir"], self.config[m]["file"])]
        self.output_dir = self.config[self.module]["output_dir"]

    async def run(self):
        
        logging.info(f'Run Module: "{self.module}", manual: "{self.manual}", batch_date: "{self.batch_date}", store_tmp: "{self.store_tmp}, write_mode: "{self.write_mode}"')
        
        result = {"module": self.module, "task": "Completed"}
        try:
            await self.check_source_files()
            # await self.retrieve_data_from_source_files()
            # # await self.mapping_column()
            # await self.mock_data()
            # await self.write_data_to_tmp_file()
        
        # except Exception as err:
        except CustomException as error: 
            
            logging.error("Error CustomException")
            result.update({"task": "Uncompleted"})
            # while True:
            #     try:
            #         logging.error(next(error))
            #     except StopIteration:
            #         break
                
        logging.info("Stop Run Module\n##### End #####\n")
        
        return result
        
    
    async def mapping_column(self) -> None:
        
        state = "failed"
        for record in self.logging:
            record.update({"function": "mapping_module_adm", "state": state})
            try:
                for sheet, data in record["data"].items():
                    logging.info(f'Mapping Column From Sheet: "{sheet}"')
                    
                    if "ADM" in sheet:
                        print(data)
                        
            except Exception as err:
                record.update({'errors': err})
                
                
    async def mock_data(self) -> None:
            mock_data = [['ApplicationCode',	'AccountOwner', 'AccountName',	'AccountType',	'EntitlementName',	'SecondEntitlementName','ThirdEntitlementName', 'AccountStatus',	'IsPrivileged',	'AccountDescription',
                        'CreateDate','LastLogin','LastUpdatedDate',	'AdditionalAttribute'],
                        [1,2,3,4,5,6,7,8,9,10,self.batch_date.strftime('%Y-%m-%d'),12, self.date,14],
                        [15,16,17,18,19,20,21,22,23,24,self.batch_date.strftime('%Y-%m-%d'),26, self.date,28],
                        ]
            df = pd.DataFrame(mock_data)
            df.columns = df.iloc[0].values
            df = df[1:]
            df = df.reset_index(drop=True)
            self.logging.append({'module': 'Target_file', 'data': df.to_dict('list')})
