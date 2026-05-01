"""Tests for builtins.py - built-in functions."""

import pytest

from daggerml._internal.builtins import BUILTIN_FNS


class TestBuiltinFunctions:
    """Test built-in function implementations."""

    def test_builtin_fns_defined(self):
        """Test that BUILTIN_FNS contains expected functions."""
        expected_functions = ["get", "contains", "list", "dict", "assoc", "conj", "unnest"]
        for func_name in expected_functions:
            assert func_name in BUILTIN_FNS

    @pytest.mark.parametrize(
        "fn_name,args,expected",
        [
            # get with string keys for dict
            ("get", ({"x": 42}, "x"), 42),
            ("get", ({"x": 42}, "y", 100), 100),
            # slice
            ("get", ([10, 20, 30, 40, 50], [1, 4]), [20, 30, 40]),
            # get with string keys for dict
            ("get", ({"a": 1, "b": 2}, "c", 0), 0),
            # contains
            ("contains", ({"a": 1, "b": 2}, "a"), True),
            ("contains", ({"a": 1, "b": 2}, "c"), False),
            ("contains", ([1, 2, 3], 2), True),
            ("contains", ([1, 2, 3], 4), False),
            # constructors
            ("list", (1, 2, 3), [1, 2, 3]),
            ("dict", ("a", 1, "b", 2), {"a": 1, "b": 2}),
            # assoc
            ("assoc", ({"a": 1}, "b", 2), {"a": 1, "b": 2}),
            ("assoc", ({"a": 1, "b": 2}, "b", 3), {"a": 1, "b": 3}),
            # conj
            ("conj", ([1, 2], 3), [1, 2, 3]),
            # unnest
            ("unnest", [[[1, 2], [3, 4]]], [1, 2, 3, 4]),
            ("unnest", [[[1, 2], [3, [4]]]], [1, 2, 3, [4]]),  # note it's not flattened
        ],
    )
    def test_builtin_dispatch_returns_expected_values(self, fn_name, args, expected):
        """Test behavior of individual built-in functions."""
        fn = BUILTIN_FNS[fn_name]
        result = fn(*args)
        assert result == expected

    @pytest.mark.parametrize(
        "fn_name,args,expected_error,error_message",
        [
            # get function errors - lists
            ("get", ([1, 2, 3], 0, "default"), TypeError, "Default values not supported for list access"),
            ("get", ([1, 2, 3], [1]), ValueError, "Slice key must have exactly 2 elements"),
            ("get", ([1, 2, 3], [1, "2"]), TypeError, "Slice indices must be integers"),
            ("get", ([1, 2, 3], "not_int"), TypeError, "List indices must be integers"),
            # get function errors - dicts
            ("get", ({"a": 1}, [1, 2]), TypeError, "Dict keys must be strings"),
            ("get", ({"a": 1}, 42), TypeError, "Dict keys must be strings"),
            # get function errors - other types
            ("get", (42, "key"), TypeError, "Cannot get from object of type int, expected list or dict"),
            ("get", ("string", 0), TypeError, "Cannot get from object of type str, expected list or dict"),
            # contains function errors
            ("contains", (42, 1), TypeError, "Cannot check contains on object of type int, expected list or dict"),
            # dict function errors
            ("dict", ("a", 1, "b"), ValueError, "Dict requires an even number of arguments"),
            ("dict", ([1, 2], 3, 4, 5), TypeError, "Invalid key-value pairs for dict"),
            # assoc function errors
            ("assoc", (42, "key", "value"), TypeError, "Cannot assoc on object of type int, expected dict"),
            ("assoc", ([1, 2, 3], "key", "value"), TypeError, "Cannot assoc on object of type list, expected dict"),
            # conj function errors
            ("conj", (42, 1), TypeError, "Cannot conj on object of type int, expected list"),
            ("conj", ({"a": 1}, 1), TypeError, "Cannot conj on object of type dict, expected list"),
        ],
    )
    def test_sad_path(self, fn_name, args, expected_error, error_message):
        """Test error handling of built-in functions."""
        fn = BUILTIN_FNS[fn_name]
        with pytest.raises(expected_error, match=error_message):
            fn(*args)
