# Crux 3DF Spike

*Broken for now. Move to module system in progress.*

This is currently out of date and pokes around with the internals of
Crux, but is still somewhat working.

This spike relies on an old version of Crux living on this branch, so
first install it from the repository root:

```
cd ../../
lein install
```

From another terminal, build and run the declarative-dataflow v0.2.0
server directly from git:

```
git clone https://github.com/comnik/declarative-dataflow/tree/v0.2.0
git checkout v0.2.0
cd server
cargo build
cargo run
```

(This example project also contains a Dockerfile and a
docker-compose.yml, but they are using HEAD and not pinned to a
specific version of declarative-dataflow.)

Start a Clojure REPL and require the `crux-3df.core` namespace, this
will try to connect to the above server, so it needs to be running.

There's a comment at the bottom of this namespace, which can be
evaluated, from `exec!` and downwards. My impression is that this
doesn't fully work, but there's some output.

## TODO

This is only to bring it more inline with the current version of Crux,
nothing for the functionality itself yet:

* Use a gateway submit function and don't decorate the system.
* Remove the hook and take listen to the tx-log instead.
* Bring up-to-date with master.
