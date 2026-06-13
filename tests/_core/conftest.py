from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.fixture
def fake_dml():
    with patch("daggerml._core.Dml", autospec=True) as mock_dml:
        yield mock_dml
