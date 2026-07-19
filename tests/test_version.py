"""The public and package-metadata versions must never drift."""

from importlib.metadata import version

import serif


def test_public_version_matches_installed_package_metadata():
    assert serif.__version__ == version('serif')
