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


def test_state_copy() -> None:
    state = TaskiqState()
    values = [1]
    state.items_list = values

    copied = state.copy()

    assert isinstance(copied, TaskiqState)
    assert dict(state) == dict(copied) == {"items_list": values}
    assert copied["items_list"] is values
    copied.other = 2
    assert "other" not in state


def test_state_copy_preserves_data_key() -> None:
    state = TaskiqState()
    state["data"] = "user value"

    copied = state.copy()

    assert dict(state) == dict(copied) == {"data": "user value"}
    copied["data"] = "copied value"
    assert state["data"] == "user value"
