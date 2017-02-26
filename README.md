happyreaper
===========

This is a command line client for [cassandra-reaper](https://github.com/thelastpickle/cassandra-reaper).

It is a self contained binary that you can deploy anywhere without needing a working Python environment.

Installation
------------

Either get a release from the [releases page](https://github.com/vrischmann/happyreaper/releases) or build it.

Building
--------

To build happyreaper you need a [Go](https://golang.org) installation.

Then you can install it with this command:

```go
go get github.com/vrischmann/happyreaper
```

Features
--------

It maps almost all Reaper endpoints, notably missing are:
  * `GET /ping` (which I'm not sure is that useful here)
  * `PUT /{cluster_name}` to modify seeds for a cluster. Not hard to add but I haven't had the need yet.
