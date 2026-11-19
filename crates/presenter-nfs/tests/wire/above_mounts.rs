//! LOOKUP in the volume root above a backend's mount prefix, which no
//! backend holds: a missing name answers `NFS3ERR_NOENT` (RFC 1813, 3.3.3).

use anyhow::{Result, ensure};
use cascade_engine::content::transfer::TransferConfig;
use cascade_presenter_nfs::nfs::server::NfsCacheMode;
use nfs3_client::nfs3_types::nfs3::{nfs_fh3, nfsstat3};

use crate::support::client::{Client, code};
use crate::support::fixture::{Fixture, Layout, NAS};

macro_rules! above_a_named_mount_tests {
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

above_a_named_mount_tests! {
    lookup_of_a_missing_name_in_the_volume_root_answers_noent_and_the_mount_stays_found
        => missing_names_in_the_volume_root;
}

async fn lookups(nfs: &mut Client, dir: &nfs_fh3, names: &[&str]) -> Result<Vec<nfsstat3>> {
    let mut statuses = Vec::with_capacity(names.len());
    for name in names {
        statuses.push(code(&nfs.lookup(dir, name).await?));
    }
    Ok(statuses)
}

async fn missing_names_in_the_volume_root(cached: bool) -> Result<()> {
    let fixture = Fixture::build(
        TransferConfig::default(),
        Layout::Named { cached },
        NfsCacheMode::default(),
    )
    .await?;
    fixture.mock.add_file("top.txt", b"top");
    let mut nfs = fixture.client().await?;
    let root = nfs.root();

    let missing = [".DS_Store", "absent", "top.txt"];
    assert_eq!(
        lookups(&mut nfs, &root, &missing).await?,
        [nfsstat3::NFS3ERR_NOENT; 3],
        "LOOKUP of {missing:?} in the volume root"
    );

    let nas = nfs.lookup(&root, NAS).await?;
    ensure!(nas.is_ok(), "LOOKUP {NAS} from the root: {nas:?}");
    let nas = nfs.walk(NAS).await?;
    assert_eq!(
        code(&nfs.lookup(&nas, "absent").await?),
        nfsstat3::NFS3ERR_NOENT,
        "LOOKUP of absent under {NAS}"
    );
    Ok(())
}
