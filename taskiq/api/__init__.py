"""
Package to run some taskiq functions programmatically.

This package is useful for managing dynamically created brokers and
scheduler instances.
"""

from taskiq.api.receiver import run_receiver_task
from taskiq.api.scheduler import run_scheduler_task

__all__ = ["run_receiver_task", "run_scheduler_task"]
