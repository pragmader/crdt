# Last-Writer-Wins State-based Directional Graph

## Introduction

[Conflict-Free Replicated Data Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) (CRDTs) are data structures that power real-time collaborative applications in distributed systems. CRDTs can be replicated across systems, they can be updated independently and concurrently without coordination between the replicas, and it is always mathematically possible to resolve inconsistencies that might result.

## Reading

* https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type
* https://github.com/pfrazee/crdt_notes
* https://hal.inria.fr/inria-00555588/PDF/techreport.pdf

## Implementation

This is a state-based LWW-Element-Graph implementation with test cases.
This includes implementation of a LWW-Element-Set which is composed into the graph for storing vertices and edges.

The graph contains functionalities to:
* add a vertex/edge
* remove a vertex/edge,
* check if a vertex is in the graph,
* query for all vertices connected to a vertex,
* find any path between two vertices,
* merge with concurrent changes from other graph/replica.

## Running tests

You need to have docker installed in order to run the tests.

Use `make test` command to run the tests.

## Author

MIT License

[Denis Rechkunov](https://pragmader.me) mail@pragmader.me
