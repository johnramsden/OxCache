# Testing

There\'s a custom test harness in `integration_tests.org`. It will run
when `cargo test` is invoked. It also dispatches with different config
options based on the environment variable `RUNNING_IN_GITHUB_ACTIONS`.
`eval_client.rs` will connect and try to run a workload.

I intended mock~device~ to be a lightweight simulation of the actual
device that provides better validation and information for debugging,
but it\'s pretty tightly coupled with the other modules so I\'m not sure
how much of a benefit this is. Data validation would have to be done by
the client in `eval_client.rs`.
