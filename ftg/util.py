from banal import clean_dict as _clean_dict
from banal import is_mapping


def clean_dict(data, expensive=False):
    """make sure empty strings are None"""
    data = {k: v or None for k, v in data.items()}
    if expensive:
        # more expensive, use with caution
        for k, v in data.items():
            if isinstance(v, str):
                data[k] = v.strip()
            if is_mapping(v):
                data[k] = clean_dict(v, expensive)
    return _clean_dict(data)


def clean_list(data):
    return [i for i in data if i]


def unique_list(data):
    return list(set(clean_list(data)))


def prefixed_dict(data, prefix=""):
    return {f"{prefix}_{k}": v for k, v in data.items()}


class cached_property:
    # https://gist.github.com/koirikivi/c58d30fce18ac1f0d65f06bfa4f93743
    # https://docs.djangoproject.com/en/3.2/_modules/django/utils/functional/#cached_property
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.

    A cached property can be made out of an existing method:
    (e.g. ``url = cached_property(get_absolute_url)``).
    The optional ``name`` argument is obsolete as of Python 3.6 and will be
    deprecated in Django 4.0 (#30127).
    """
    name = None

    @staticmethod
    def func(instance):
        raise TypeError(
            "Cannot use cached_property instance without calling "
            "__set_name__() on it."
        )

    def __init__(self, func, name=None):
        self.real_func = func
        self.__doc__ = getattr(func, "__doc__")

    def __set_name__(self, owner, name):
        if self.name is None:
            self.name = name
            self.func = self.real_func
        elif name != self.name:
            raise TypeError(
                "Cannot assign the same cached_property to two different names "
                "(%r and %r)." % (self.name, name)
            )

    def __get__(self, instance, cls=None):
        """
        Call the function and put the return value in instance.__dict__ so that
        subsequent attribute access on the instance returns the cached value
        instead of calling cached_property.__get__().
        """
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res
