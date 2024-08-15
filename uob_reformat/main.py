import asyncio
from .module_adm import ModuleADM
from .module_bos import ModuleBOS
from .module_cum import ModuleCUM
from .module_doc import ModuleDOC
from .module_dil import ModuleDIL
from .module_ica import ModuleICA
from .module_iic import ModuleIIC
from .module_lds import ModuleLDS
from .module_lmt import ModuleLMT
from .module_moc import ModuleMOC
from .setup import PARAMS, setup_folder, setup_log, clear_log
from datetime import datetime


class RunModule:
    def __init__(self) -> None:
        for key, value in PARAMS.items():
            setattr(self, key, value)
        self.date = datetime.now()

        setup_folder()
        setup_log()
        clear_log()

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.results = self.loop.run_until_complete(self.mapping_module())

    async def mapping_module(self):
        coros = []
        for module in self.source:
            setattr(self, "module", module)

            if module == "ADM":
                tasks = ModuleADM(self)
                run = asyncio.create_task(tasks.run_process())
                coros.append(run)

            elif module == "BOS":
                tasks = ModuleBOS(self)
                run = asyncio.create_task(tasks.run_process())
                coros.append(run)

            elif module == "CUM":
                tasks = ModuleCUM(self)
                run = asyncio.create_task(tasks.run_process())
                coros.append(run)

            elif module == "DOC":
                tasks = ModuleDOC(self)
                run = asyncio.create_task(tasks.run_process())
                coros.append(run)

            elif module == "DIL":
                tasks = ModuleDIL(self)
                run = asyncio.create_task(tasks.run_process())
                coros.append(run)

            elif module == "ICA":
                tasks = ModuleICA(self)
                run = asyncio.create_task(tasks.run_process())
                coros.append(run)

            elif module == "IIC":
                tasks = ModuleIIC(self)
                run = asyncio.create_task(tasks.run_process())
                coros.append(run)

            elif module == "LDS":
                tasks = ModuleLDS(self)
                run = asyncio.create_task(tasks.run_process())
                coros.append(run)

            elif module == "LMT":
                tasks = ModuleLMT(self)
                run = asyncio.create_task(tasks.run_process())
                coros.append(run)

            elif module == "MOC":
                tasks = ModuleMOC(self)
                run = asyncio.create_task(tasks.run_process())
                coros.append(run)

        return await asyncio.wait(coros)


class StartApp(RunModule):
    pass
