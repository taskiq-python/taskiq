"""Minimal exception templating used by taskiq exceptions."""

from string import Formatter


class Error(Exception):
    """Base templated exception compatible with taskiq needs."""

    __template__ = "Exception occurred"

    @classmethod
    def _collect_annotations(cls) -> dict[str, object]:
        """Collect all annotated fields from the class hierarchy."""
        annotations: dict[str, object] = {}
        for class_ in reversed(cls.__mro__):
            annotations.update(getattr(class_, "__annotations__", {}))
        return annotations

    @classmethod
    def _format_fields(cls, names: set[str]) -> str:
        """Format field names in a deterministic error message."""
        return ", ".join(f"'{name}'" for name in sorted(names))

    @classmethod
    def _template_fields(cls, template: str) -> set[str]:
        """Extract plain field names used in a format template."""
        fields: set[str] = set()
        for _, field_name, _, _ in Formatter().parse(template):
            if not field_name:
                continue
            field = field_name.split(".", maxsplit=1)[0].split("[", maxsplit=1)[0]
            fields.add(field)
        return fields

    def __init__(self, **kwargs: object) -> None:
        annotations = self._collect_annotations()
        undeclared = set(kwargs) - set(annotations)
        if undeclared:
            raise TypeError(f"Undeclared arguments: {self._format_fields(undeclared)}")

        missing = {
            field
            for field in annotations
            if field not in kwargs and not hasattr(type(self), field)
        }
        if missing:
            raise TypeError(f"Missing arguments: {self._format_fields(missing)}")

        for key, value in kwargs.items():
            setattr(self, key, value)

        template = getattr(type(self), "__template__", self.__template__)
        missing_annotations = self._template_fields(template) - set(annotations)
        if missing_annotations:
            raise ValueError(
                f"Fields must be annotated: {self._format_fields(missing_annotations)}",
            )

        payload = {field: getattr(self, field) for field in annotations}
        super().__init__(template.format(**payload))

    def __repr__(self) -> str:
        """Represent exception with all declared fields."""
        annotations = self._collect_annotations()
        module = type(self).__module__
        qualname = type(self).__qualname__
        if not annotations:
            return f"{module}.{qualname}()"
        args = ", ".join(f"{field}={getattr(self, field)!r}" for field in annotations)
        return f"{module}.{qualname}({args})"
