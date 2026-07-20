# Serif Design Philosophy

Serif is intentionally small, predictable, and Pythonic.  
Its goal is not to compete with full-fledged analytics engines, but to provide a clean, minimal foundation for vectors, tables, and lightweight transformations that integrate naturally with Python and the UI systems built on top of it.

This document outlines the core principles that guide the design of Serif.  
Any future feature or contribution should be evaluated against these principles.

---

## **Principle 1 — Minimal Surface Area; Maximal Composability**

Serif prioritizes a small, understandable set of primitives rather than an ever-expanding API.  
Every additional method or behavior must *earn its place* by providing clear value and interacting cleanly with existing semantics.

- Prefer a single, general mechanism over multiple convenience wrappers.
- Favor composition of simple operations rather than adding new operators.
- Avoid implicit magic or hidden transformations.
- If a feature can be built from existing pieces, it probably should be.

Serif's strength comes from being small, predictable, and easy to reason about.

---

## **Principle 2 — Vector Semantics First, Table Semantics Built on Top**

Vector treats vectors as the core abstraction.  
Tables, matrices, and higher-dimensional structures are built by composing vectors, not by inventing an entirely separate conceptual layer.

- Columns are vectors; tables are collections of column-vectors.
- Operations on tables should feel like operations on aligned vectors.
- Slicing, masking, mapping, and arithmetic derive naturally from vector behavior.
- 2D structure must never violate 1D consistency.

This ensures that Vector has a coherent mathematical foundation, and that tables remain predictable rather than becoming a separate “galaxy” of special rules.

---

## **Principle 3 — Pythonic by Default; Strict Where Ambiguity Creates Risk**

Serif follows Python’s scalar and operator semantics wherever they are intuitive, predictable, and safe. This ensures that vectorized expressions behave naturally for users who already understand Python.

However, when Python’s legacy behaviors lead to surprising or harmful outcomes—particularly in contexts involving boolean masks, filtering, or data selection—the library introduces minimal, targeted restrictions.

Examples:

- Boolean values participate in arithmetic (`True + 7 → 8`, `b * b → 1`) because this is both Pythonic and useful.  
- Logical operators preserve boolean behavior (`&`, `|`, `^`, `~`).  
- But ambiguous or misleading operations, such as unary boolean negation (`-b`), are explicitly disallowed because they rarely reflect user intent and can silently corrupt masks.

The goal is not to redefine Python, but to inherit its behaviors where they make sense and correct only the cases where they do not. This principle helps keep the system intuitive, predictable, and safe for analytical workflows.

---

## **Principle 4 — Explicit Is Better Than Implicit**

Ambiguity leads to bugs. Serif avoids “cleverness” that conceals meaning.

- No automatic type inference beyond what Python already does.
- No silent coercions except those Python performs natively.
- Column names map directly to columns, and nothing else.
- Masking is always explicit.
- Indexing rules are strict, predictable, and documented.

This keeps Serif stable as datasets grow and transforms become more complex.

---

## **Principle 5 — Representation Should Reinforce Understanding**

Every Vector or Table should print in a form that:

- gives useful structural information,
- shows representative data,
- fits naturally in notebooks and REPLs,
- avoids flooding the screen.

Formatting is not decoration—it is part of the feedback loop that enables rapid exploration.

If the printed output does not help the user understand what they have, it should be fixed.

---

## **Principle 6 — No Surprises Across Data Sizes**

Behavior should not change between:

- a 5-element vector,
- a 500-element vector,
- or a 5,000,000-element vector.

This includes:

- slicing,
- masking,
- arithmetic,
- type promotion,
- and table-column alignment.

Serif preserves semantics consistently regardless of scale.  
Performance optimizations should not introduce semantic differences.

---

## **Principle 7 — Predictability Over Performance**

Serif values correctness, clarity, obviousness, and exploration speed over
micro-optimized performance.

Serif is the exploratory, expressive, human-scale layer: the place where
you think. Performance work is welcome, but never at the cost of
semantics — a faster path that behaves differently at scale is a bug
(see Principle 6).

---

## **Principle 8 — Fewer Concepts, Better Concepts**

Whenever possible, Serif favors:

- one way to do a thing,
- orthogonal building blocks,
- clear invariants,
- consistent return types.

If adding a feature forces the user to learn another “mini-language” to understand it, the feature doesn’t belong.

---

# Summary

Serif philosophy is straightforward:

- Keep the system small, composable, and Pythonic.  
- Let vectors define table behavior.  
- Avoid ambiguity, especially around boolean masks.  
- Show users clear, helpful representations.  
- Maintain predictable semantics at any scale.  
- Value predictability and exploration speed over micro-optimization.  
- Choose clarity and minimalism over kitchen-sink convenience.

Any future addition should be judged on whether it strengthens or weakens these principles.


