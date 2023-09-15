from typing import Annotated

from taskiq import TaskiqDepends


async def db_connection() -> str:
    return "let's pretend as this is a connection"


class MyDAO:
    def __init__(self, db_conn: Annotated[str, TaskiqDepends(db_connection)]) -> None:
        self.db_conn = db_conn

    def get_users(self) -> str:
        return self.db_conn.upper()


def my_task(dao: Annotated[MyDAO, TaskiqDepends()]) -> None:
    print(dao.get_users())
