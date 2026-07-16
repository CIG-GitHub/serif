# Representation Rules (repr)

The repr of vectors and tables is designed for human inspection,
not machine round-tripping.

## Vector repr
A vector repr includes:
- first n entries (default 5)
- last n entries (default 5)
- vertical alignment for numeric clarity
- ellipsis if truncated
- footer: `# <length> element <dtype> vector`

Example:
    1
    2
    3
    4
    5
    ...
  995
  996
  997
  998
  999

  # 1000 element vector <int>

## Table repr
A table repr includes:
- column headers
- aligned rows (head/tail)
- ellipsis row separating head/tail
- footer showing shape and a grouped dtype summary

Column names appear literally, not sanitized.

Example:

col_a   col_b
    1   2025-10-31
    2   2025-10-31
    3   2025-10-31
  ...   ...
  999   2025-10-31
 1000   2025-10-31

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

## Principles
- repr should be unambiguous and legible  
- it should communicate shape, dtype, and sample values  
- it should never attempt to show full large data structures  
- it must remain stable across versions  
- output should resemble notebook head/tail conventions  

