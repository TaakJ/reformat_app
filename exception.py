class CustomException(Exception):
    def __init__(self, **kwargs: dict):
        self.__dict__.update(kwargs)
        
        for key, value in self.__dict__.items():
            setattr(self, key, value)
        
        self.msg_err = self.generate_error_message()
    
    def __iter__(self):
        return self

    def __next__(self):
        return next(self.msg_err)

    def generate_error_message(self):
        for i in  range(len(self.errors)):
            error_message = f"Module::{self.errors[i]['module']} - Path::{self.errors[i]['input_dir']} - Function::{self.errors[i]['function']} - State::{self.errors[i]['state']} - Error::{self.errors[i].get('errors')}"
            yield error_message