happyreaper
===========

This is a command line client for [cassandra-reaper](https://github.com/thelastpickle/cassandra-reaper).

It is a self contained binary that you can deploy anywhere without needing a working Python environment.

Features
--------

It maps almost all Reaper endpoints, notably missing are:
  * `GET /ping` (which I'm not sure is that useful here)
  * `PUT /{cluster_name}` to modify seeds for a cluster. Not hard to add but I haven't had the need yet.
