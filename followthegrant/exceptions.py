class LoaderException(Exception):
    ...


class ParserException(Exception):
    ...


class TaskException(Exception):
    ...


class InnerTaskException(TaskException):
    ...


class IdentificationException(Exception):
    ...


class ModelException(Exception):
    ...


class TransformException(Exception):
    ...
