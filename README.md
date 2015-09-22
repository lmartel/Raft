# ToyBoat - A Raft implementation

[Raft](https://raft.github.io/) is a distributed consensus algorithm. Consensus algorithms provide strong consistency guarantees to a cluster of servers, coordinating agreement on a value or safely replicating a state machine. Raft is mostly-equivalent to an earlier algorithm called Paxos, but was designed specifically to be easier to understand and leave less as an "exercise for the reader." You can read [the original paper](http://ramcloud.stanford.edu/raft.pdf) for an excellent description of the algorithm and its many uses. You can also read [any Paxos paper](http://research.microsoft.com/en-us/um/people/lamport/pubs/paxos-simple.pdf) to see why an easier and more exaustively-specified algorithm was necessary.

This repo contains my own implementation of Raft, in Haskell. I've implemented the full protocol, the two big pieces being log-entry propagation and leader election. Performance was not my focus, but it's fast enough to be IO-bound (and constrained by the configurable heartbeat/election timers) rather than CPU-bound. I also skipped space optimizations like log compaction, since I'm not using this in production anywhere and the log files take a long time to get huge.


## Quick Start

Installing haskell is a huge pain for no reason, so if you don't have it I'd recommend just downloading the repo with precompiled binaries from the GitHub release page. If you just want to run the whole thing and see what's going on, here's how to do that:

```bash
# run the default cluster in three terminal tabs.
# when a prompt pops up in one of them, type numbers in to submit log entries.
./raft-osx 1
./raft-osx 2
./raft-osx 3
# to see what's going on under the hood, open 3 more terminal tabs:
tail -100f log/debug.1.log
tail -100f log/debug.2.log
tail -100f log/debug.3.log
```

It'll look like this: ![ToyBoat Raft demo](raft-demo.png?raw=true)


### Running ToyBoat

Here's the full usage information:

```bash
./raft-osx SERVER_ID [COMMAND]   # commands = [log, test, leader, candidate, follower]
./raft-linux SERVER_ID [COMMAND] # commands = [log, test, leader, candidate, follower]

Examples:
./raft-osx 1           # runs a Raft instance with ID=1
./raft-osx 3 candidate     # runs a Raft instance with ID=3, forcing it into a particular starting role (candidate)
./raft-osx 2 log           # dumps a nicely-formatted version of Server 2's write-ahead log, found in db/log.ID.json.
./raft-osx 1 test          # runs the tests for Server 1. The test harness has been set up, but there aren't many tests.
```

The config file, `conf/config.json`, sets up a 3-node cluster to run locally. You can manually edit the JSON to change this, if you like.


### Compiling ToyBoat

ToyBoat compiles and runs on OSX and Linux. The repo includes a Rakefile for simplifying the compilation commands as well as managing Docker if you're on OSX but want the Linux version.

You need Ruby and Rake to build the project. Maybe someday I'll learn to enjoy writing Makefiles, but for now, google `installing RVM on [MY OS]`.

You need Haskell too (obviously). Installing it is hard but Google-able. The project uses GHC version 7.10. Once you have Haskell, `cabal build` will install the project's dependencies.

Finally, you're ready to actually compile the damn project. Assuming you're running OSX `rake compile` will compile for OSX, and `rake docker:build; rake docker:compile` will compile for Linux using Docker.


#### Using the Rakefile

Full list of Rakefile commands, for convenience:

```bash
rake                # default: compile for both OSX and Linux
rake reset          # reset the state machine, clear the write-ahead logs
rake log[ID]        # dump the server log for server #ID; alias for ./raft-OS ID log
rake debug[ID]      # tail the log file for server #ID
rake compile        # compile for OSX
rake run[ID]        # run server #ID; alias for ./raft-OS ID

rake docker:build   # build a linux image with Haskell and Raft's dependencies
rake docker:compile # compile for Linux using docker
rake docker:run[ID] # run server #ID on Linux inside docker
```
