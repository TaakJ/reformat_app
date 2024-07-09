class CustomException(Exception):
    def __init__(self,*args: tuple,**kwargs: dict,):
        self.__dict__.update(kwargs)

        for key, value in self.__dict__.items():
            setattr(self,key,value)

        self.err_msg = self.generate_error()

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.err_msg)

    def generate_error(self) -> any:
        for i in range(len(self.err)):
            module      = self.err[i]["module"]
            full_input  = self.err[i].get("input_dir")
            full_target = self.err[i].get("full_target")
            status      = self.err[i]["status"]
            func        = self.err[i]["function"]
            err         = self.err[i].get("err")

            if err is not None:
                err_msg = f'Module: {module}; File: {full_target}; Function: {func}; ERROR: {err}'
                yield err_msg
