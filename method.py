import asyncio
from module import convert_2_files
from verify import method_files
from mapping import module_adm, module_bos, module_cum


class run_module(convert_2_files, method_files, module_adm, module_bos, module_cum):
    pass

class start(run_module):
    def __init__(self, params: dict):
        self.params = params
        
        run_module = asyncio.get_event_loop()
        run_module.run_until_complete(self.mapping_source_files())
        
    async def mapping_source_files(self) -> None:
        
        for source in self.params["source"]:
            if source == "ADM":
                run_task = self.run_module_adm(self.params)
                
            elif source == "BOS":
                run_task = self.run_module_bos(self.params)
                
            elif source == "CUM":
                run_task = self.run_module_cum(self.params)
            
            asyncio.create_task(run_task)
            
            
            