"""Internal callable identity helpers for reusable task declarations."""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Callable
from types import FunctionType
from typing import Any, ParamSpec, TypeVar, cast

_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")

_BINDING_MARKER = "__taskiq_binding_function__"
_BINDING_SOURCE = "__taskiq_binding_source__"
_EMPTY_CELL = object()


def _values_match(left: object, right: object) -> bool:
    """Compare identity or recursively stable immutable values."""
    if left is right:
        return True
    if type(left) is not type(right):
        return False
    return _same_type_values_match(left, right)


def _same_type_values_match(left: object, right: object) -> bool:
    """Compare supported immutable values after exact type validation."""
    if type(left) in {str, bytes, int, bool, type(None)}:
        return bool(left == right)
    if isinstance(left, float) and isinstance(right, float):
        return left.hex() == right.hex()
    if isinstance(left, tuple) and isinstance(right, tuple):
        return len(left) == len(right) and all(
            _values_match(left_item, right_item)
            for left_item, right_item in zip(left, right, strict=True)
        )
    if isinstance(left, frozenset) and isinstance(right, frozenset):
        return _frozensets_match(left, right)
    return False


def _frozensets_match(
    left: frozenset[object],
    right: frozenset[object],
) -> bool:
    """Compare unordered immutable values without relying on loose equality."""
    unmatched_values = list(right)
    if len(left) != len(unmatched_values):
        return False
    for left_item in left:
        for index, right_item in enumerate(unmatched_values):
            if _values_match(left_item, right_item):
                unmatched_values.pop(index)
                break
        else:
            return False
    return True


def _mappings_match(
    left: dict[str, Any] | None,
    right: dict[str, Any] | None,
) -> bool:
    """Compare keyword defaults without trusting mutable value equality."""
    if left is right:
        return True
    if left is None or right is None or left.keys() != right.keys():
        return False
    return all(_values_match(left[key], right[key]) for key in left)


def _closure_values(func: FunctionType) -> tuple[object, ...]:
    """Return closure cell contents while preserving empty-cell identity."""
    values = []
    for cell in func.__closure__ or ():
        try:
            values.append(cell.cell_contents)
        except ValueError:
            values.append(_EMPTY_CELL)
    return tuple(values)


def _execution_state_matches(left: object, right: object) -> bool:
    """Return whether two runtime functions execute equivalent captured state."""
    if not isinstance(left, FunctionType) or not isinstance(right, FunctionType):
        return False
    if left.__code__ is not right.__code__ or left.__globals__ is not right.__globals__:
        return False
    if not _values_match(left.__defaults__, right.__defaults__):
        return False
    if not _mappings_match(left.__kwdefaults__, right.__kwdefaults__):
        return False
    if not _mappings_match(left.__annotations__, right.__annotations__):
        return False
    return _values_match(_closure_values(left), _closure_values(right))


def ensure_task_binding_function(
    func: Callable[_FuncParams, _ReturnType],
    *,
    binding_key: str,
) -> Callable[_FuncParams, _ReturnType]:
    """Return a stable module-level callable without mutating ``func``."""
    module_name = getattr(func, "__module__", type(func).__module__)
    function_qualname = getattr(func, "__qualname__", type(func).__qualname__)
    identity_module = (
        "__main__" if module_name in {"__main__", "__mp_main__"} else module_name
    )
    function_code = getattr(func, "__code__", None)
    first_line = getattr(function_code, "co_firstlineno", 0)
    identity = f"{identity_module}\0{function_qualname}\0{first_line}\0{binding_key}"
    digest = hashlib.blake2s(identity.encode(), digest_size=10).hexdigest()
    export_name = f"_taskiq_shared_{digest}"
    module = sys.modules[module_name]

    existing = getattr(module, export_name, None)
    existing_source = getattr(existing, _BINDING_SOURCE, None)
    if existing_source is func:
        return cast(Callable[_FuncParams, _ReturnType], existing)
    if existing is not None and _execution_state_matches(existing_source, func):
        return cast(Callable[_FuncParams, _ReturnType], existing)
    if existing is not None and (
        existing_source is None
        or getattr(existing_source, "__code__", None) is function_code
    ):
        raise ValueError(
            f"Task binding identity collision for {binding_key!r}. "
            "Factory-generated TaskDefinitions must use unique task names.",
        )

    if isinstance(func, FunctionType):
        binding_function = FunctionType(
            func.__code__,
            func.__globals__,
            export_name,
            func.__defaults__,
            func.__closure__,
        )
        binding_function.__kwdefaults__ = func.__kwdefaults__
    else:

        def forwarding_function(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        binding_function = cast(FunctionType, forwarding_function)

    binding_function.__module__ = module_name
    binding_function.__name__ = export_name
    binding_function.__qualname__ = export_name
    binding_function.__doc__ = getattr(func, "__doc__", None)
    binding_function.__annotations__ = dict(getattr(func, "__annotations__", {}))
    binding_function.__dict__.update(getattr(func, "__dict__", {}))
    binding_function.__dict__["__wrapped__"] = func
    setattr(binding_function, _BINDING_SOURCE, func)
    setattr(binding_function, _BINDING_MARKER, True)
    setattr(module, export_name, binding_function)
    return cast(Callable[_FuncParams, _ReturnType], binding_function)


def is_task_binding_function(func: Callable[..., Any]) -> bool:
    """Return whether ``func`` already has a stable process-pool identity."""
    return bool(getattr(func, _BINDING_MARKER, False))
