from log import record_log
from exception import CustomException
from setup import Folder, clear_tmp
import logging


class module_adm(record_log):
        
    def _log_setter(self, log):
        self._log = log
        
    async def run_module_adm(self, param):  
        
        for key, value in param.items():
            setattr(self, key, value)
        
        self.state = True
        logging.info(f'Run Module: "ADM", manual: "{self.manual}" batch_date: "{self.batch_date}", store_tmp: "{self.store_tmp}, write_mode: "{self.write_mode}"')
        
        try:
            self.source = ["ADM"]
            await self.check_source_files()
            await self.retrieve_data_from_source_files()
                
        except CustomException as error: 
            logging.error("Error Exception")
            self.state = False
            
            while True:
                try:
                    logging.error(next(error))
                except StopIteration:
                    break
        
        logging.info("Stop Run Module\n##### End #####\n")
        
    
    
class module_bos(record_log):
    
    def _log_setter(self, log):
        self._log = log
        
    async def run_module_bos(self, param):  
        
        for key, value in param.items():
            setattr(self, key, value)
        
        self.state = True
        logging.info(f'Run Module: "BOS", manual: "{self.manual}" batch_date: "{self.batch_date}", store_tmp: "{self.store_tmp}, write_mode: "{self.write_mode}"')
        try:
            self.source = ["BOS"]
            await self.check_source_files()
            await self.retrieve_data_from_source_files()
                
        except CustomException as error: 
            logging.error("Error Exception")
            self.state = False
            
            while True:
                try:
                    logging.error(next(error))
                except StopIteration:
                    break
        
        logging.info("Stop Run Module\n##### End #####\n")
        
    
    
class module_cum(record_log):
    
    async def run_module_cum(self, param):  
        
        for key, value in param.items():
            setattr(self, key, value)
        
        self.state = True
        logging.info(f'Run Module: "CUM", manual: "{self.manual}" batch_date: "{self.batch_date}", store_tmp: "{self.store_tmp}, write_mode: "{self.write_mode}"')
        
        try:
            self.source = ["CUM"]              
            await self.check_source_files()
            await self.retrieve_data_from_source_files()
                
        except CustomException as error: 
            logging.error("Error Exception")
            self.state = False
            
            while True:
                try:
                    logging.error(next(error))
                except StopIteration:
                    break
        
        logging.info("Stop Run Module\n##### End #####\n")

    