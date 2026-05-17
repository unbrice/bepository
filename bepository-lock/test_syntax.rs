use futures::stream::{StreamExt, TryStreamExt};
use std::future::Future;

async fn test() -> Result<(), std::io::Error> {
    let metas = vec![1, 2, 3];
    futures::stream::iter(metas)
        .map(|meta| async move {
            if meta == 2 {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "test"));
            }
            Ok(())
        })
        .buffer_unordered(100)
        .try_for_each(|_| async { Ok(()) })
        .await?;
    Ok(())
}
