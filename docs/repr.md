# Representation Rules (repr)

The repr of vectors and tables is designed for human inspection,
not machine round-tripping.

## Vector repr
A vector repr includes:
- first n entries (default 6)
- last n entries (default 6)
- vertical alignment for numeric clarity
- ellipsis if truncated
- footer: `# <length> element vector <dtype>`

The head/tail count is `set_repr_rows() // 2`; the default of 12 total
rows can be changed with `serif.set_repr_rows(n)`.

Example:
       1
       2
       3
       4
       5
       6
     ...
     995
     996
     997
     998
     999
    1000

    # 1000 element vector <int>

## Table repr
A table repr includes:
- column headers (original names, quoted when needed; a `.dot_name` row
  when sanitization changed anything; a `[dtype]` row when columns are
  heterogeneous)
- aligned rows (head/tail)
- ellipsis row separating head/tail
- footer showing shape and a grouped dtype summary

Example:

    col_a  col_b
    [int]  [date]
        1  2025-10-31
        2  2025-10-31
        3  2025-10-31
      ...  ...
      999  2025-10-31
     1000  2025-10-31

    # 1000×2 table <int, date>

### Footer dtype summary
Per-column dtypes are aggregated into counted groups in column order
(first appearance), so the footer reads like the table does. A count
prefix is used when a dtype repeats in a heterogeneous table; a
homogeneous table shows the bare dtype. With five or more dtype groups,
the first three are shown and the remaining columns fold into `+N`:

    # 1000000×2 table <int>
    # 1000000×3 table <str, int, date>
    # 1000000×9 table <6×str, 2×int, date>
    # 1000000×40 table <18×str, 12×int, 6×float, +4>

## Schema listing: `t._`

`t._` prints one row per column — dot-accessor, dtype, and the original
name where sanitization changed it:

    .first_name   str   'first name'
    .price        int   'price ($)'
    .total        int

It reads column metadata only (never scans data) and shows every column,
up to 1000. See docs/naming.md for how accessors are derived.

## Principles
- repr should be unambiguous and legible  
- it should communicate shape, dtype, and sample values  
- it should never attempt to show full large data structures  
- it must remain stable across versions  
- output should resemble notebook head/tail conventions  

