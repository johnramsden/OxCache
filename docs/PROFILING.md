# Profiling

Determine PID of thread. Can use:

```aiignore
cargo add libc
```

```rust
use libc;
fn get_tid() -> libc::pid_t {
    unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t }
}
```

Perf has a bug on 24.04:

```
WARNING: perf not found for kernel 6.14.0-28

  You may need to install the following packages for this specific kernel:
    linux-tools-6.14.0-28-generic
    linux-cloud-tools-6.14.0-28-generic

  You may also want to install one of the following packages to keep up to date:
    linux-tools-generic
    linux-cloud-tools-generic
```

Use `/usr/lib/linux-tools/6.8.0-64-generic/perf`

```
sudo /usr/lib/linux-tools/6.8.0-64-generic/perf record -F 199 -g -t 11823
```

Then, for flamegraph:

```
sudo /usr/lib/linux-tools/6.8.0-64-generic/perf script | \
    /data/john/FlameGraph/stackcollapse-perf.pl | \
    /data/john/FlameGraph/flamegraph.pl > flame.svg
```