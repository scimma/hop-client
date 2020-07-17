from unittest.mock import patch, mock_open

import pytest

from hop import auth


def test_load_auth(auth_config):
    with patch("builtins.open", mock_open(read_data=auth_config)) as mock_file:

        # check error handling
        with pytest.raises(FileNotFoundError):
            auth.load_auth()

        # check auth loads correctly
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            auth.load_auth()
