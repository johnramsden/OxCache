use tokio::runtime::Handle;

pub fn execute_async<'a, F, Return>(future: F) -> Return
where
    F: Future<Output = Return> + Send + 'a,
    Return: Send + 'a,
{
    let handle = Handle::current();
    handle.block_on(future)
}
