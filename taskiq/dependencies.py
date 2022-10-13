import inspect
from asyncio import iscoroutine
from collections import defaultdict, deque
from copy import copy
from graphlib import TopologicalSorter
from typing import (  # noqa: WPS235
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Dict,
    Generator,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    get_type_hints,
    overload,
)

_T = TypeVar("_T")  # noqa: WPS111


@overload
def TaskiqDepends(  # noqa: WPS234
    dependency: Optional[Callable[..., AsyncGenerator[_T, None]]] = None,
    *,
    use_cache: bool = True,
    kwargs: Optional[Dict[str, Any]] = None,
) -> _T:
    ...


@overload
def TaskiqDepends(  # noqa: WPS234
    dependency: Optional[Callable[..., Generator[_T, None, None]]] = None,
    *,
    use_cache: bool = True,
    kwargs: Optional[Dict[str, Any]] = None,
) -> _T:
    ...


@overload
def TaskiqDepends(
    dependency: Optional[Type[_T]] = None,
    *,
    use_cache: bool = True,
    kwargs: Optional[Dict[str, Any]] = None,
) -> _T:
    ...


@overload
def TaskiqDepends(  # noqa: WPS234
    dependency: Optional[Callable[..., Coroutine[Any, Any, _T]]] = None,
    *,
    use_cache: bool = True,
    kwargs: Optional[Dict[str, Any]] = None,
) -> _T:
    ...


@overload
def TaskiqDepends(
    dependency: Optional[Callable[..., _T]] = None,
    *,
    use_cache: bool = True,
    kwargs: Optional[Dict[str, Any]] = None,
) -> _T:
    ...


def TaskiqDepends(
    dependency: Optional[Any] = None,
    *,
    use_cache: bool = True,
    kwargs: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Constructs a dependency.

    This function returns TaskiqDepends
    and needed for typehinting.

    :param dependency: function to run as a dependency.
    :param use_cache: whether the dependency
        can use previously calculated dependencies.
    :param kwargs: optional keyword arguments to the dependency.
        May be used to parametrize dependencies.
    :return: TaskiqDepends instance.
    """
    return _TaskiqDepends(
        dependency=dependency,
        use_cache=use_cache,
        kwargs=kwargs,
    )


class _TaskiqDepends:
    """
    Class to mark parameter as a dependency.

    This class is used to mark parameters of a function,
    or a class as injectables, so taskiq can resolve it
    and calculate before execution.
    """

    def __init__(  # noqa: WPS234
        self,
        dependency: Optional[Union[Type[Any], Callable[..., Any]]] = None,
        *,
        use_cache: bool = True,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.dependency = dependency
        self.use_cache = use_cache
        self.param_name = ""
        self.kwargs = kwargs or {}

    def __hash__(self) -> int:
        return hash((self.dependency, self.use_cache, tuple(self.kwargs.keys())))

    def __eq__(self, rhs: object) -> bool:
        """
        Overriden eq operation.

        This is required to perform correct topological
        sort after building dependency graph.

        :param rhs: object to compare.
        :return: True if objects are equal.
        """
        if not isinstance(rhs, _TaskiqDepends):
            return False
        return (self.dependency, self.use_cache, self.kwargs) == (
            rhs.dependency,
            rhs.use_cache,
            rhs.kwargs,
        )


class DependencyResolveContext:
    """
    Resolver context.

    This class is used to resolve dependencies
    with custom initial caches.

    The main idea is to separate resolving and graph building.
    It uses graph, but it doesn't modify it.
    """

    def __init__(
        self,
        graph: "DependencyGraph",
        initial_cache: Optional[Dict[Any, Any]] = None,
    ) -> None:
        self.graph = graph
        self.opened_dependencies: List[Any] = []
        self.sub_contexts: List["DependencyResolveContext"] = []
        self.initial_cache = initial_cache or {}

    async def __aenter__(self) -> "DependencyResolveContext":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:  # noqa: C901
        """
        Close all opened dependencies.

        This function runs teardown of all dependencies.
        """
        for ctx in self.sub_contexts:
            await ctx.close()
        for dep in reversed(self.opened_dependencies):
            if inspect.isgenerator(dep):
                for _ in dep:  # noqa: WPS328
                    pass  # noqa: WPS420
            elif inspect.isasyncgen(dep):
                async for _ in dep:  # noqa: WPS328
                    pass  # noqa: WPS420

    async def resolve_kwargs(  # noqa: C901, WPS210
        self,
    ) -> Dict[str, Any]:
        """
        Resolve dependencies and return them as a dict.

        This function runs all dependencies
        and calculates key word arguments required to run target function.

        :return: Dict with keyword arguments.
        """
        # If we have nothing to calculate, we return
        # an empty dict.
        if self.graph.is_empty():
            return {}
        kwargs: Dict[str, Any] = {}
        # We need to copy cache, in order
        # to separate dependencies that use cache,
        # from dependencies that aren't.
        cache = copy(self.initial_cache)
        # We iterate over topologicaly sorted list of dependencies.
        for index, dep in enumerate(self.graph.ordered_deps):
            # If this dependency doesn't use cache,
            # we don't need to calculate it, since it may be met
            # later.
            if not dep.use_cache:
                continue
            # If somehow we have dependency with unknwon function.
            if dep.dependency is None:
                continue
            # If dependency is already calculated.
            if dep.dependency in cache:
                continue
            kwargs = {}
            # Now we get list of dependencies for current top-level dependency
            # and iterate over it.
            for subdep in self.graph.dependencies[dep]:
                # If we don't have known dependency function,
                # we skip it.
                if subdep.dependency is None:
                    continue
                if subdep.use_cache:
                    # If this dependency can be calculated, using cache,
                    # we try to get it from cache.
                    kwargs[subdep.param_name] = cache[subdep.dependency]
                else:
                    # If this dependency doesn't use cache,
                    # we resolve it's dependencies and
                    # run it.
                    subctx = self.graph.subgraphs[subdep].ctx(self.initial_cache)
                    # Add this graph resolve context to the list of subcontexts.
                    # We'll close it later.
                    self.sub_contexts.append(subctx)
                    resolved_kwargs = await subctx.resolve_kwargs()
                    if subdep.kwargs:
                        resolved_kwargs.update(subdep.kwargs)
                    subdep_exec = subdep.dependency(**resolved_kwargs)
                    if inspect.isgenerator(subdep_exec):
                        sub_result = next(subdep_exec)
                        self.opened_dependencies.append(subdep_exec)
                    elif iscoroutine(subdep_exec):
                        sub_result = await subdep_exec
                    elif inspect.isasyncgen(subdep_exec):
                        sub_result = await subdep_exec.__anext__()  # noqa: WPS609
                        self.opened_dependencies.append(subdep_exec)
                    else:
                        sub_result = subdep_exec

                    kwargs[subdep.param_name] = sub_result
            # We don't want to calculate least function,
            # Because it's a target function.
            if index < len(self.graph.ordered_deps) - 1:
                user_kwargs = dep.kwargs
                user_kwargs.update(kwargs)
                cache_param = dep.dependency(**user_kwargs)
                if inspect.isgenerator(cache_param):
                    result = next(cache_param)
                    self.opened_dependencies.append(cache_param)
                elif iscoroutine(cache_param):
                    result = await cache_param
                elif inspect.isasyncgen(cache_param):
                    result = await cache_param.__anext__()  # noqa: WPS609
                    self.opened_dependencies.append(cache_param)
                else:
                    result = cache_param
                cache[dep.dependency] = result
        return kwargs


class DependencyGraph:
    """Class to build dependency graph from a function."""

    def __init__(
        self,
        target: Callable[..., Any],
    ) -> None:
        self.target = target
        # Ordinary dependencies with cache.
        self.dependencies: Dict[Any, List[_TaskiqDepends]] = defaultdict(list)
        # Dependencies without cache.
        # Can be considered as sub graphs.
        self.subgraphs: Dict[Any, DependencyGraph] = {}
        self.ordered_deps: List[_TaskiqDepends] = []
        self._build_graph()

    def is_empty(self) -> bool:
        """
        Checks that target function depends on at least something.

        :return: True if depends.
        """
        return len(self.ordered_deps) <= 1

    def ctx(
        self,
        initial_cache: Optional[Dict[Any, Any]] = None,
    ) -> DependencyResolveContext:
        """
        Create dependency resolver context.

        This context is used to actually resolve dependencies.

        :param initial_cache: initial cache dict.
        :return: new resolver context.
        """
        return DependencyResolveContext(
            self,
            initial_cache,
        )

    def _build_graph(self) -> None:  # noqa: C901, WPS210
        """
        Builds actual graph.

        This function collects all dependencies
        and adds it the the _deps variable.

        After all dependencies are found,
        it runs topological sort, to get the
        dependency resolving order.

        :raises ValueError: if something happened.
        """
        dep_deque = deque([_TaskiqDepends(self.target, use_cache=True)])

        while dep_deque:
            dep = dep_deque.popleft()
            # Skip adding dependency if it's already present.
            if dep in self.dependencies:
                continue
            if dep.dependency is None:
                continue
            # Get signature and type hints.
            sign = inspect.signature(dep.dependency)
            if inspect.isclass(dep.dependency):
                # If this is a class, we need to get signature of
                # an __init__ method.
                hints = get_type_hints(dep.dependency.__init__)  # noqa: WPS609
            else:
                # If this is function, we get it's type hints.
                hints = get_type_hints(dep.dependency)

            # Now we need to iterate over parameters, to
            # find all parameters, that have TaskiqDepends as it's
            # default vaule.
            for param_name, param in sign.parameters.items():
                # We check, that default value is an instance of
                # TaskiqDepends.
                if not isinstance(param.default, _TaskiqDepends):
                    continue

                # If user haven't set the dependency,
                # using TaskiqDepends constructor,
                # we need to find variable's type hint.
                if param.default.dependency is None:
                    if hints.get(param_name) is None:
                        # In this case, we don't know anything
                        # about this dependency. And it cannot be resolved.
                        dep_mod = "unknown"
                        dep_name = "unknown"
                        if dep.dependency is not None:
                            dep_mod = dep.dependency.__module__
                            if inspect.isclass(dep.dependency):
                                dep_name = dep.dependency.__class__.__name__
                            else:
                                dep_name = dep.dependency.__name__
                        raise ValueError(
                            f"The dependency {param_name} of "
                            f"{dep_mod}:{dep_name} cannot be resolved.",
                        )
                    # We get dependency class from typehint.
                    dependency_func = hints[param_name]
                else:
                    # We can get dependency by simply using
                    # user supplied function.
                    dependency_func = param.default.dependency

                # Now we construct new TaskiqDepends instance
                # with correct dependency function and cache.
                dep_obj = _TaskiqDepends(
                    dependency_func,
                    use_cache=param.default.use_cache,
                    kwargs=param.default.kwargs,
                )
                # Also we set the parameter name,
                # it will help us in future when
                # we're going to resolve all dependencies.
                dep_obj.param_name = param_name

                # We append current dependency
                # to the list of dependencies of
                # the current function.
                self.dependencies[dep].append(dep_obj)
                if dep_obj.use_cache:
                    # If this dependency uses cache, we need to resolve
                    # it's dependencies further.
                    dep_deque.append(dep_obj)
                else:
                    # If this dependency doesn't use caches,
                    # we build a subgraph for this dependency.
                    self.subgraphs[dep_obj] = DependencyGraph(
                        dependency_func,
                    )
        # Now we perform topological sort of all dependencies.
        # Now we know the order we'll be using to resolve dependencies.
        self.ordered_deps = list(TopologicalSorter(self.dependencies).static_order())
