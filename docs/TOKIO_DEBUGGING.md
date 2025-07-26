# Tokio Debugging

For debugging issues with tokio, such as locks that are currently acquired within the runtime you can use the tokio-console. Uncomment initialization of it in `main.rs`.

To install the binary:

``` 
cargo install tokio-console
``` 

(make sure `~/.cargo/bin` is in your path.

Compile with:

``` 
RUSTFLAGS=--cfg tokio_unstable 
```