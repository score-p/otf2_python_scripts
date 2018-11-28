Combine all OTF2 traces in a folder into one.

- It will remove the `global_clock_offset` from all traces
- Allows syncing of time by adding `__syncTime` ParamterInt64 events in a function `__init` with the global time in nanoseconds (usefull for combining related traces)
- Combines regions and nodes with same name

# Requirements
- `>= Python 2.7`
- `>= OTF2 2.1 with python bindings`
- `future`

# Usage
`combineTraces.py --input <folder> --output <folder> [--clean]`

# TODO
- write tests
- Allow custom function for combining regions etc. (see `prettify_names`)

# License
BSD-2-Clause
