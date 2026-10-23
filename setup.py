"""Build hook for serif's optional compiled kernels.

All project metadata lives in pyproject.toml. This file only decides whether
the Rust extension ``serif._rust`` is built, from the SERIF_RUST variable:

    optional (default)  build it if possible; a failed build still installs
                        a working pure-Python serif that simply declines
    required            build it or fail (CI and release wheels)
    off                 never build it (the pure-Python wheel and CI jobs)
"""

import os

from setuptools import setup
from setuptools_rust import Binding
from setuptools_rust import RustExtension


mode = os.environ.get("SERIF_RUST", "optional").strip().lower()
if mode not in ("optional", "required", "off"):
    raise SystemExit(
        f"SERIF_RUST must be optional, required or off; got {mode!r}"
    )

rust_extensions = []
if mode != "off":
    rust_extensions.append(
        RustExtension(
            "serif._rust",
            path="rust/Cargo.toml",
            binding=Binding.PyO3,
            optional=mode == "optional",
            # Editable installs would otherwise build unoptimized debug code.
            debug=False,
        )
    )

setup(rust_extensions=rust_extensions)
