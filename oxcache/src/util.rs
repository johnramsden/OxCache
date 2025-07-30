#[macro_export]
macro_rules! log_async_write_lock {
    ($lock:expr) => {{
        println!(
            "[{}, {}] Trying to write lock {}",
            file!(),
            line!(),
            stringify!($lock)
        );
        let g = $lock.write().await;
        println!("[{}, {}] Locked {}", file!(), line!(), stringify!($lock));
        g
    }};
}
#[macro_export]
macro_rules! log_async_mutex_lock {
    ($lock:expr) => {{
        println!(
            "[{}, {}] Trying to lock {}",
            file!(),
            line!(),
            stringify!($lock)
        );
        let g = $lock.lock().await;
        println!("[{}, {}] Locked {}", file!(), line!(), stringify!($lock));
        g
    }};
}
#[macro_export]
macro_rules! log_async_read_lock {
    ($lock:expr) => {{
        println!(
            "[{}, {}] Trying to read lock {}",
            file!(),
            line!(),
            stringify!($lock)
        );
        let g = $lock.read().await;
        println!("[{}, {}] Locked {}", file!(), line!(), stringify!($lock));
        g
    }};
}
