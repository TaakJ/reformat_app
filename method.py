import asyncio
from module_adm import module_adm
from module_bos import module_bos
from module_cum import module_cum


class run_module:
    def __init__(self, params:dict):
        self._params = params
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.mapping_module())
        
    async def mapping_module(self):
        ''
        # coros = []
        # for module in self._params["source"]:
        #     if module == "ADM":
        #         tasks  = module_adm()
        #         tasks._params(module, self._params)
        #         coros.append(asyncio.create_task(tasks.run()))
                
        #     elif module == "BOS":
        #         tasks = module_bos()
        #         tasks._params(module, self._params)
        #         coros.append(asyncio.create_task(tasks.run()))
                
        #     elif module == "CUM":
        #         tasks  = module_cum()
        #         tasks._params(module, self._params)
        #         coros.append(asyncio.create_task(tasks.run()))
                
        # results = await asyncio.wait(coros)
        # print(f'Run Task: {len(results[0])}')
        # [print(f'- Task:: {completed_task.result()["module"]} - Status:: {completed_task.result()["task"]}')\
        #     for completed_task in results[0]]    
        
class start_app(run_module):
    pass
            
            