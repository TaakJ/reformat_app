import asyncio
from module import convert_2_files
from verify import method_files
from module_adm import module_adm
from module_bos import module_bos
from module_cum import module_cum


class start:
    def __init__(self, params:dict):
        self.params = params
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.mapping_source())

    async def mapping_source(self):
        for source in self.params["source"]:
            
            if source == "ADM":
                task = module_adm().run(source)
                
            elif source == "BOS":
                task = module_bos().run(source)
                
            elif source == "CUM":
                task = module_cum().run(source)
        
            asyncio.create_task(task)
        
class run_module(start):
    pass
            
            