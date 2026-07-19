# Releasing Serif

## Prepare the release branch

1. Create `feature/releaseX.Y.Z` from the tested release candidate.
2. Set the same version in `pyproject.toml` and `serif.__version__`.
3. Finalize the matching changelog section.
4. Confirm documentation describes the dependency and behavior contracts of
   the release.
5. Push the branch and wait for every CI environment to pass.

CI is the release gate. It tests Python 3.10–3.14 with no accelerators,
NumPy only, PyArrow only, and both installed; tests the declared minimum
accelerator versions; and validates the source and wheel distributions.

## Merge and tag

Merge the release branch into `main`, then tag that exact merge commit using
the repository's existing `vX.Y.Z` convention:

```bash
git switch main
git pull --ff-only
git tag -a vX.Y.Z -m "Serif X.Y.Z"
git push origin vX.Y.Z
```

Never move a published release tag. If an artifact is wrong after publishing,
fix it in a new version.

## Build and publish

Build from the clean tagged commit so the GitHub and package-index artifacts
come from identical source:

```bash
python -m pip install --upgrade build twine
python -m build
python -m twine check dist/*
python -m pip install --force-reinstall --no-deps dist/*.whl
python -c "import serif; print(serif.__version__)"
```

Publish those exact artifacts to the package index and attach them to the
GitHub release with the matching changelog section as its release notes.
