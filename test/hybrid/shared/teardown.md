# Teardown

Appended to every hybrid document: stops the daemon, unmounts, terminates
the run's Mutagen session, removes the run's directory on the share and its
temporary directories, then lists whatever outlived them.

```scrut
$ hybrid_teardown && hybrid_leftovers
```
