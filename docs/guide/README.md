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
It was the main reason we created this project. It's still in the early stages of development,
so it's not a production-ready solution yet. We use it at work, but still, you may encounter bugs.
If your project requires mature solutions, please use Dramatiq or Celery instead.

Also, this project is not for you if you have a fully synchronous project.
