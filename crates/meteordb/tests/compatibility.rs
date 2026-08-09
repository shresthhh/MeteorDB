use meteordb::{DATABASE_FORMAT_VERSION, PUBLIC_API_VERSION, SSTABLE_FORMAT_VERSION};

#[test]
fn published_compatibility_versions_are_generation_one() {
    assert_eq!(PUBLIC_API_VERSION, 1);
    assert_eq!(DATABASE_FORMAT_VERSION, 1);
    assert_eq!(SSTABLE_FORMAT_VERSION, 2);
}
