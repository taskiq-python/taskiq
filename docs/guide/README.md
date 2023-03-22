---
order: 1
dir:
  order: 1
---

# Introduction

## What is taskiq

Taskiq is a library that helps you send and process python functions in a distributed manner.
For example, you have many heavy to calculate functions you want to execute on another server.
You can implement interservice communication by yourself, or you can use Taskiq to make the job done easily.

The core library has only two brokers. It provides CLI, basic functionality for creating tasks, and abstractions to extend functionality. The main idea is to make taskiq modular with a lot of
replaceable parts, such as brokers, result backends, and middlewares.

## Why not use existing libraries?

We couldn't find a solution like Celery or Dramatiq that can run async code and send tasks asynchronously.
It was the main reason we created this project.

You might have seen projects built on top of asyncio that solve similar problem, but here's a comparasion table of taskiq and other projects.

|                Feature name | Taskiq |  Arq  | AioTasks |
| --------------------------: | :----: | :---: | :------: |
|         Actively maintained |   ✅    |   ✅   |    ❌     |
|    Multiple broker backends |   ✅    |   ❌   |    ✅     |
|    Multiple result backends |   ✅    |   ❌   |    ❌     |
|  Have a rich documentation  |   ✅    |   ❌   |    ❌     |
|   Startup & Shutdown events |   ✅    |   ✅   |    ❌     |
| Have ability to abort tasks |   ❌    |   ✅   |    ❌     |
|          Custom serializers |   ✅    |   ✅   |    ❌     |
|        Dependency injection |   ✅    |   ❌   |    ❌     |
|              Task pipelines |   ✅    |   ✅   |    ❌     |
|              Task schedules |   ✅    |   ✅   |    ❌     |
|          Global middlewares |   ✅    |   ❌   |    ❌     |

If you have a fully synchronous project, consider using celery or dramatiq instead.
