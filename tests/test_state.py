from taskiq.state import TaskiqState


def test_state_set() -> None:
    """Tests that you can sel values as dict items."""
    state = TaskiqState()
    state["a"] = 1

    assert state["a"] == 1


def test_state_get() -> None:
    """Tests that you can get values as dict items."""
    state = TaskiqState()

    state["a"] = 1

    assert state["a"] == 1


def test_state_del() -> None:
    """Tests that you can del values as dict items."""
    state = TaskiqState()

    state["a"] = 1

    del state["a"]

    assert state.get("a") is None


def test_state_set_attr() -> None:
    """Tests that you can set values by attribute."""
    state = TaskiqState()

    state.a = 1

    assert state["a"] == 1


def test_state_get_attr() -> None:
    """Tests that you can get values by attribute."""
    state = TaskiqState()

    state["a"] = 1

    assert state.a == 1


def test_state_del_attr() -> None:
    """Tests that you can delete values by attribute."""
    state = TaskiqState()

    state["a"] = 1

    del state.a

    assert state.get("a") is None
