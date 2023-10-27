from taskiq.compat import IS_PYDANTIC2

from .cron_spec import CronSpec

if IS_PYDANTIC2:
    from .v2 import ScheduledTask
else:
    from .v1 import ScheduledTask  # type: ignore


__all__ = ["CronSpec", "ScheduledTask"]
