import logging

def debug_args(func):
    def debug_wrapper(*args, **kwargs):
        logging.NOCOMMIT(f"function: {func.__name__}, args: {args}, kwargs: {kwargs}")
        res = func(*args, **kwargs)
        logging.NOCOMMIT(f"function: {func.__name__}, return type: {type(res)}")
        return res

    return debug_wrapper
