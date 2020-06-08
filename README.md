# raftsies

[![CircleCI](https://circleci.com/gh/redshiftzero/raftsies.svg?style=svg)](https://circleci.com/gh/redshiftzero/raftsies)

This is a fault-tolerant key/value storage system using Raft for distributed consensus. The implemention is pure Python without any 3rd party dependencies (but test dependencies are in `dev-requirements.txt`).

:warning: I implemented this for learning and fun only, definitely do not use this in a production context. I started working on this during David Beazley's excellent Raft class ([check it out here](https://www.dabeaz.com/raft.html), you should take it!).

## Running the raft cluster

Run in separate Python interpreters (can do on a single host for testing) or on separate hosts:

```
./run.py <node_num>
```

where `<node_num>` is unique and corresponds to the entries in `config.json`.

```
./run.py 0
./run.py 1
./run.py 2
./run.py 3
./run.py 4
```

To start a node in a particular server state (`FOLLOWER`, `CANDIDATE` or `LEADER`) for debugging purposes:

```
./debug.py <node_num> LEADER
```

Configuration is stored in `config.json`. An example config is committed. For demo purposes you may want to slow down the elections (so it's easier to follow the node messages), e.g. making the election start and stop times longer, but note that the heartbeat time should be faster than the shortest election time.

`make clean` removes all the persistent raft state, including the transaction log. This is useful for testing purposes.

## Interact with cluster as a client

```
./example_client.py
```

## Development

```
pip install -r dev-requirements.txt
make test  # unsurprisingly this run tests
make lint  # runs linter and type checker
```
