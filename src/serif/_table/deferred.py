"""Deferred boolean-mask Table coordination."""

from .._vector.selection import popcount
from .._vector.storage import TupleStorage
from ..table import Table
from . import columns as _columns


class MaskedTable(Table):
    """
    Deferred boolean-mask selection — what `t[mask]` returns outside a
    batch() scope. Gathers a column only when someone asks for it.

    Why a snapshot is sound: the storage protocol is rebuild-only and the
    mutation doctrine makes owner-addressed writes swap-on-write — no code
    anywhere mutates a frozen column's storage in place. Capturing each
    column at the storage level at defer time is therefore a permanently
    frozen snapshot: `q = t[t.a > 10]` means "t as it was", forever, with
    no version counters. (The one exception — batch() scopes write into
    private buffers in place — never reaches here: __getitem__ keeps the
    eager path while `t._unlocked`.)

    Row is the existence proof for the shape: a hollow subclass that
    bypasses __init__ and exposes `_storage` as a materialize-on-demand
    property, so every base-class method works unmodified. Hot paths
    (attribute access, string subscripts) gather a single column, cached;
    everything else falls through the `_storage` property, which
    materializes all columns and LATCHES — after the first full access
    the object behaves as a plain Table permanently. Derived results are
    plain Tables/Vectors; the deferred type never escapes as a result.

    Eager at defer time (so error timing and observable state don't
    move): mask type/length validation (in __getitem__) and the survivor
    popcount — len() and .shape are exact from birth.
    """

    # Class-level defaults: an instance that skipped __init__ must read
    # as latched-with-nothing, not recurse through __getattr__ on its
    # own deferral state.
    _mat = None
    _captured = None
    _gathered = None
    _mask_vec = None
    _source_loader = None

    def __new__(cls, source, mask):
        # Bypass Table.__new__/__init__ entirely (see Row).
        return object.__new__(cls)

    def __init__(self, source, mask):
        capture = getattr(type(source), '_mask_capture', None)
        # The source's column map may be stale (a column aliased after
        # construction). The eager path rebuilds names from the gathered
        # columns implicitly; refresh here so the shared map matches —
        # the same lazy rebuild Table.__getattr__ performs.
        if capture is None:
            if any(col._wild for col in source._storage):
                source._column_map = source._build_column_map()

        # Capture at the storage level: private shells sharing each
        # frozen storage O(1) — never the source Table or its column
        # objects, whose names can mutate in place via alias().
        if capture is None:
            captured = tuple(col.copy() for col in source._storage)
            source_loader = None
        else:
            captured, source_loader = capture(source)
        mask_shell = mask.copy()

        # Survivor popcount, eager: len()/shape stay exact and cheap.
        n = popcount(mask_shell._storage)

        # Deferral state. Table.__setattr__ would route these through
        # column lookup, so bind them raw.
        object.__setattr__(self, '_captured', captured)
        object.__setattr__(self, '_mask_vec', mask_shell)
        object.__setattr__(self, '_gathered', {})
        object.__setattr__(self, '_mat', None)
        object.__setattr__(self, '_source_loader', source_loader)

        # Slot checklist for bypassing Table.__init__ — mirror
        # _from_columns_nocopy. The column map is REUSED from the source
        # (identical names by construction; never mutated in place, only
        # rebound). _warned_collisions carries as a copy so collision
        # warnings the source already fired don't re-fire on a post-latch
        # map rebuild — and a rebuild on our side can't mark the source.
        object.__setattr__(self, '_dtype',      None)
        object.__setattr__(self, '_name',       None)
        object.__setattr__(self, '_wild',       False)
        object.__setattr__(self, '_repr_rows',  None)
        object.__setattr__(self, '_length',     n)
        object.__setattr__(self, '_column_map', source._column_map)
        object.__setattr__(self, '_warned_collisions',
                           set(source._warned_collisions))

    # ------------------------------------------------------------------
    # The deferred core: per-column gather + the materialize-and-latch
    # ------------------------------------------------------------------

    def _gather_column(self, idx):
        """Gather (and cache) one column through the captured snapshot.

        Runs the exact per-column program of the old eager path —
        shell[mask] takes the accel filter or the pure zip-filter — so
        results are identical by construction. Cached: the snapshot is
        frozen, so `q.b + q.b` must not gather twice. The gathered column
        is table-owned, hence tamed and frozen, exactly what
        _build_column_map does to every eager table's columns.
        """
        col = self._gathered.get(idx)
        if col is None:
            if self._source_loader is None:
                col = self._captured[idx][self._mask_vec]
            else:
                col = self._source_loader(idx, self._mask_vec)
            col._wild = False
            col._frozen = True
            self._gathered[idx] = col
        return col

    @property
    def _storage(self):
        """Materialize every column and latch: from here on, every
        base-class method sees a plain Table. Assembled from the same
        cached objects the hot paths handed out, so identity behaves
        like a real Table's. The snapshot is released — a latched
        MaskedTable no longer pins the source buffers."""
        mat = self._mat
        if mat is None:
            cols = tuple(self._gather_column(i)
                         for i in range(len(self._captured)))
            mat = TupleStorage.from_iterable(cols, nullable=False)
            object.__setattr__(self, '_mat', mat)
            self._release_snapshot()
        return mat

    @_storage.setter
    def _storage(self, value):
        # Post-latch rebinds (_write_column, column replacement) land
        # here via Table.__setattr__'s object.__setattr__, which honors
        # data descriptors. A rebind IS a latch: whatever storage the
        # caller installed is now the whole truth.
        object.__setattr__(self, '_mat', value)
        if self._captured is not None:
            self._release_snapshot()

    def _release_snapshot(self):
        object.__setattr__(self, '_captured', None)
        object.__setattr__(self, '_gathered', None)
        object.__setattr__(self, '_mask_vec', None)
        object.__setattr__(self, '_source_loader', None)

    def _snapshot_names_current(self):
        """Gathered columns are handed out live (cached) — a rename
        through one (alias(), the wild mechanic) makes the captured map
        stale, the very condition Table.__getattr__ repairs with a
        rebuild. Detect it and decline the deferred shortcut: the Table
        path latches, rebuilds the map, and fires any collision warning,
        exactly as an eager table would."""
        gathered = self._gathered
        if not gathered:
            return True
        return not any(col._wild for col in gathered.values())

    # ------------------------------------------------------------------
    # Hot paths: single-column access without materializing
    # ------------------------------------------------------------------

    def __getattr__(self, attr):
        # Plain column names (and col{N}_ spellings — the captured map
        # holds those too) gather one column. Everything else — indexed
        # accessors ('name__5'), method fallbacks, a stale map — takes
        # Table's path, which may materialize; correct by default.
        if self._mat is None and self._snapshot_names_current():
            col_idx = self._column_map.get(attr)
            if col_idx is None:
                col_idx = self._column_map.get(attr.lower())
            if col_idx is not None:
                return self._gather_column(col_idx)
        return Table.__getattr__(self, attr)

    def __getitem__(self, key):
        if self._mat is None and self._snapshot_names_current():
            if isinstance(key, str):
                return self._gather_column(
                    _columns.resolve_column_key(self._captured, key))
            if isinstance(key, tuple) and all(isinstance(k, str) for k in key):
                # Multi-column selection: gather only the named columns.
                # Table() copies each (O(1) share), same as the eager path.
                return Table([self[col_name] for col_name in key])
        return Table.__getitem__(self, key)

    def cols(self, key=None):
        # Positional single-column access gathers just that column;
        # cols() / cols(slice) return several, so they materialize.
        if self._mat is None and isinstance(key, int):
            idx = key if key >= 0 else key + len(self._captured)
            if not (0 <= idx < len(self._captured)):
                raise IndexError(
                    f"Column index {key} out of range (table has "
                    f"{len(self._captured)} columns)")
            return self._gather_column(idx)
        return Table.cols(self, key)

    # ------------------------------------------------------------------
    # Cheap introspection: exact from the eager popcount, no gathering
    # ------------------------------------------------------------------

    def __len__(self):
        if self._mat is None:
            return self._length
        return Table.__len__(self)

    @property
    def shape(self):
        if self._mat is None:
            n_cols = len(self._captured)
            return (self._length if n_cols else 0, n_cols)
        return Table.shape.fget(self)

    def column_names(self):
        if self._mat is None and self._snapshot_names_current():
            return [col._name for col in self._captured]
        return Table.column_names(self)

    def _schema_columns(self):
        if self._mat is None and self._snapshot_names_current():
            return self._captured
        return Table._schema_columns(self)

