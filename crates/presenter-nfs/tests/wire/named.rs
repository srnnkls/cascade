//! A backend mounted at its id, as the daemon mounts it, served over `NFSv3`
//! with and without a content cache attached.

use anyhow::Result;
use cascade_engine::content::transfer::TransferConfig;
use cascade_presenter_nfs::nfs::server::NfsCacheMode;
use nfs3_client::nfs3_types::nfs3::nfsstat3;

use crate::support::fixture::{Fixture, Layout, NAS, eventually};

macro_rules! named_mount_tests {
    ($($name:ident => $scenario:ident;)*) => {
        $(
            mod $name {
                #[tokio::test(flavor = "multi_thread")]
                async fn with_a_content_cache() -> anyhow::Result<()> {
                    super::$scenario(true).await
                }

                #[tokio::test(flavor = "multi_thread")]
                async fn without_a_content_cache() -> anyhow::Result<()> {
                    super::$scenario(false).await
                }
            }
        )*
    };
}

named_mount_tests! {
    lookup_of_a_named_mount_from_the_root_finds_it => lookup_of_the_mount_from_the_root;
    readdir_of_the_root_lists_a_named_mount => readdir_of_the_root;
    readdir_of_a_named_mount_lists_its_share => readdir_of_the_mount;
    lookup_and_read_of_a_file_under_a_named_mount_return_its_content => lookup_and_read_under_the_mount;
    write_to_a_file_under_a_named_mount_reaches_the_backend => write_under_the_mount;
}

async fn named_mount_with_a_share(cached: bool) -> Result<Fixture> {
    let fixture = Fixture::build(
        TransferConfig::default(),
        Layout::Named { cached },
        NfsCacheMode::default(),
    )
    .await?;
    fixture.mock.add_file("sub/hello.txt", b"hello, nas!!");
    fixture.mock.add_file("top.txt", b"top");
    Ok(fixture)
}

async fn lookup_of_the_mount_from_the_root(cached: bool) -> Result<()> {
    let fixture = named_mount_with_a_share(cached).await?;
    let mut nfs = fixture.client().await?;

    let nas = nfs.lookup(&nfs.root(), NAS).await?;

    let Ok(nas) = nas else {
        anyhow::bail!("LOOKUP {NAS} from the root: {nas:?}");
    };
    assert_eq!(
        nfs.names(&nas).await?,
        Ok(vec!["sub".to_owned(), "top.txt".to_owned()]),
        "the handle of {NAS} lists its share"
    );
    Ok(())
}

async fn readdir_of_the_root(cached: bool) -> Result<()> {
    let fixture = named_mount_with_a_share(cached).await?;
    let mut nfs = fixture.client().await?;

    assert_eq!(nfs.names(&nfs.root()).await?, Ok(vec![NAS.to_owned()]));
    Ok(())
}

async fn readdir_of_the_mount(cached: bool) -> Result<()> {
    let fixture = named_mount_with_a_share(cached).await?;
    let mut nfs = fixture.client().await?;
    let nas = nfs.walk(NAS).await?;

    assert_eq!(
        nfs.names(&nas).await?,
        Ok(vec!["sub".to_owned(), "top.txt".to_owned()])
    );
    Ok(())
}

async fn lookup_and_read_under_the_mount(cached: bool) -> Result<()> {
    let fixture = named_mount_with_a_share(cached).await?;
    let mut nfs = fixture.client().await?;

    let hello = nfs.walk("nas/sub/hello.txt").await?;

    assert_eq!(nfs.read(&hello, 0, 12).await?, b"hello, nas!!");
    Ok(())
}

async fn write_under_the_mount(cached: bool) -> Result<()> {
    let fixture = named_mount_with_a_share(cached).await?;
    let mut nfs = fixture.client().await?;
    let hello = nfs.walk("nas/sub/hello.txt").await?;

    assert_eq!(nfs.write(&hello, 7, b"NAS").await?, nfsstat3::NFS3_OK);

    assert_eq!(nfs.read(&hello, 0, 12).await?, b"hello, NAS!!");
    eventually("the write reaches the backend", || {
        fixture.on_backend("sub/hello.txt").as_deref() == Some(b"hello, NAS!!".as_slice())
    })
    .await
}
