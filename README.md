# MapReduce implementation using Python and MPI

Implementation of the distributed MapReduce Paradigm for creating the Inverted Index of any given number of .txt files.

*In addition the implementation contains also a Combining Stage and Wordkload balanced distribution of files amongst the Mapper processes.*

## Running

In order to run the script, the module MPI is needed.
Run example:
First argument is the dir with source files, the second argument the dir with the resulted files

```bash
mpiexec -n 9 python map_reduce.py test-files result-dir
```
