---
title: Task manager for asyncio
home: true
heroImage: logo.svg
heroAlt: logo
heroText: Taskiq python
actions:
  - text: Get Started
    link: /guide/getting-started.html
    type: primary
  - text: Introduction
    link: /guide/
    type: secondary
head:
  - - meta
    - name: "google-site-verification"
      content: "hQCR5w2tmeuOvYIYXsOYU3u4kLNwT86lnqltANYlRQ0"
  - - meta
    - name: "msvalidate.01"
      content: "97DC185FE0A2F5B123861F0790FDFB26"
  - - meta
    - name: "yandex-verification"
      content: "9b105f7c58cbc920"
highlights:
    - features:
        - title: Production ready
          details: Taskiq has proven that it can run in heavy production systems with high load.
        - title: Fully asynchronous
          details: Taskiq can run both sync and async functions. You don't have to worry about it.
        - title: Easily extensible
          details: Taskiq has a lot of replaceable components. It's super easy to implement
            your own broker or middleware.
        - title: Strongly typed
          details: Taskiq provides correct autocompletion for most of its functionality.

copyright: false
footer: MIT Licensed | Copyright© 2022-present
---

## What is taskiq in a nutshell

Consider taskiq as an asyncio celery implementation. It uses almost the same patterns, but it's more modern
and flexible.

It's not a drop-in replacement for any other task manager. It has a different ecosystem of libraries and a different set of features.
Also, it doesn't work for synchronous projects. You won't be able to send tasks synchronously.


## Installation

You can install taskiq with pip or your favorite dependency manager:

```bash:no-line-numbers
pip install taskiq
```
