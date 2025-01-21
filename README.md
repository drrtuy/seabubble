# SeaBubble

## Repo artifacts
`bubbles_gen` - generates files of lexigraphically sortable 4096 blocks.
`seabubble` - shared Priority Queue based 3 phase sorting. First produce multiple sorted runs, then merge separate sorted runs into <number_of_shards> merged sorted runs and finaly merge <number_of_shards> merged sorted runs into a single file.

## Build

To build applications from this repo:
```
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DSEASTAR_REPO_PATH=/git/seastar -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -B build .
cd ./build && make -j 5
```
### Prerequisites

Requires [Seastar](https://github.com/scylladb/seastar) async framework/runtime locally installed. Use `SEASTAR_REPO_PATH` to point to Seastar repo clone root.

## How to use
Use the apps help to see lists of the arguments available. Here is the example for `seabubble`.
```
./seabubble -i ./random-2000000-8GB.file -t /temp_dir/ --default-log-level=info -c 10 -m 20G
```
