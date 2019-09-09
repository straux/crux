# Crux 3DF Spike

Example of the crux-dataflow module. Uses the embedded
declarative-dataflow server from crux-dataflow. x86_64 Linux only.

(This example project also contains a Dockerfile and a
docker-compose.yml, but they are using HEAD and not pinned to a
specific version of declarative-dataflow.)

Run the example:

```
lein run
```

My impression is that this doesn't fully work, but there's some
output.

Note that the `data` directory is kept between runs and needs to be
deleted to start fresh.
