class LoaderException(Exception):
    pass


class ParserException(Exception):
    pass


class TaskException(Exception):
    pass


class InnerTaskException(TaskException):
    pass
