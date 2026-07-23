# Introduction

This Jupyter Book documents an incremental **Code Property Graph (CPG)**
construction and streaming ingestion pipeline built over the public repository
[`huggingface/optimum`](https://github.com/huggingface/optimum).

## The pipeline in one paragraph

A Parser Service reads Python source files **one at a time**, extracts AST nodes
together with control-flow, data-flow and call edges, assigns every element a
**stable, line-independent identifier**, and emits the results to four Apache
Kafka topics. Node and edge topics flow into **Neo4j** through the Neo4j
Connector for Kafka with no Spark layer in between. The metadata topic flows into
**MongoDB** through a **Spark Structured Streaming** job with checkpointing and
upsert semantics. Reprocessing any single file leaves both databases correct and
duplicate-free.

## Results from our run

Measured on commit `a6c775e` of the upstream repository:

| Metric | Value |
|---|---|
| Python files discovered | 74 |
| Source files after exclusions | 59 |
| Lines of code parsed | 13,725 |
| CPG nodes emitted | 58,817 |
| CPG edges emitted | 73,587 |
| — AST | 57,760 |
| — DFG | 8,259 |
| — CFG | 4,987 |
| — CALL | 2,581 |
| Functions / classes | 522 / 153 |
| Parse errors | 0 |

```{note}
`huggingface/optimum` is an active repository. If you re-run these notebooks
against a newer commit the counts will differ slightly. The commit hash is
printed by the first cell of Task 1 so every number in this book is traceable to
a specific tree.
```

## How to read this book

Each chapter corresponds to one task. Chapters state the approach and the
reasoning behind it, show executed cells with real outputs, embed database UI
screenshots, and close with a reflection on what worked, what failed, and how it
was resolved.

The single design decision that the whole pipeline depends on is explained in
**Task 2**: identifiers are derived from tree structure, not from line numbers.
**Task 6** shows why that choice is what makes idempotent replay possible.
