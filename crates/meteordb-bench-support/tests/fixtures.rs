use meteordb_bench_support::{
    AccessDistribution, Amplification, CACHE_VALUE_BYTES, ComponentBenchmarkReport,
    CounterSnapshot, Dataset, EMBEDDING_VALUE_BYTES, LatencySummary, WorkloadFile, WorkloadKind,
    capture_environment, compression_equivalence, measure_prepared_samples,
    read_amplification_delta, smoke_workload,
};

#[test]
fn smoke_dataset_is_reproducible_and_uses_declared_sizes() {
    let workload = smoke_workload();
    let first = Dataset::generate(&workload).unwrap();
    let second = Dataset::generate(&workload).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.keys.len(), workload.dataset.key_count);
    assert!(
        first
            .keys
            .iter()
            .all(|key| key.len() == workload.dataset.key_bytes)
    );
    assert!(first.cache_values.iter().all(|value| value.len() == 1024));
    assert!(
        first
            .embedding_values
            .iter()
            .all(|value| value.len() == 6 * 1024)
    );
    assert!(
        first
            .absent_keys
            .iter()
            .all(|key| !first.keys.contains(key))
    );
    assert_eq!(
        workload.engine.compression,
        meteordb_bench_support::CompressionConfig::None
    );
}

#[test]
fn invalid_schema_version_is_rejected() {
    let mut workload = smoke_workload();
    workload.schema_version = WorkloadFile::SCHEMA_VERSION + 1;

    let error = workload.validate().unwrap_err();
    assert!(error.to_string().contains("schema_version"));
}

#[test]
fn latency_percentiles_are_nearest_rank_and_stable() {
    let summary = LatencySummary::from_nanos(&[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]).unwrap();

    assert_eq!(summary.samples, 10);
    assert_eq!(summary.p50_ns, 50);
    assert_eq!(summary.p95_ns, 100);
    assert_eq!(summary.p99_ns, 100);
}

#[test]
fn zipfian_sampler_prefers_hot_keys_with_fixed_seed() {
    let workload = smoke_workload();
    let dataset = Dataset::generate(&workload).unwrap();
    let sampled = dataset
        .sample_indices(20_000, AccessDistribution::Zipfian { theta: 0.99 })
        .unwrap();
    let hot = sampled.iter().filter(|&&index| index < 10).count();
    let cold = sampled.iter().filter(|&&index| index >= 90).count();

    assert!(hot > cold * 5, "hot={hot}, cold={cold}");
}

#[test]
fn workload_json_round_trips_without_losing_configuration() {
    let workload = smoke_workload();
    let json = serde_json::to_string_pretty(&workload).unwrap();
    let decoded: WorkloadFile = serde_json::from_str(&json).unwrap();

    assert_eq!(decoded, workload);
}

#[test]
fn smoke_profile_covers_every_required_workload() {
    let workload = smoke_workload();
    for kind in [
        WorkloadKind::PointReadWrite,
        WorkloadKind::PrefixScan,
        WorkloadKind::RangeScan,
        WorkloadKind::InferenceCache,
        WorkloadKind::FeatureStore,
        WorkloadKind::EmbeddingStorage,
        WorkloadKind::Compaction,
    ] {
        assert!(
            workload.workloads.iter().any(|spec| spec.kind == kind),
            "missing {kind:?}"
        );
    }
}

#[test]
fn comparison_value_sizes_are_fixed() {
    let mut workload = smoke_workload();
    workload.dataset.cache_value_bytes = CACHE_VALUE_BYTES + 1;
    assert!(
        workload
            .validate()
            .unwrap_err()
            .to_string()
            .contains("1024")
    );

    workload.dataset.cache_value_bytes = CACHE_VALUE_BYTES;
    workload.dataset.embedding_value_bytes = EMBEDDING_VALUE_BYTES - 1;
    assert!(
        workload
            .validate()
            .unwrap_err()
            .to_string()
            .contains("6144")
    );
}

#[test]
fn environment_capture_records_reproducibility_fields() {
    let environment = capture_environment(env!("CARGO_MANIFEST_DIR")).unwrap();

    assert!(!environment.git_revision.is_empty());
    assert!(!environment.os.is_empty());
    assert!(!environment.architecture.is_empty());
    assert!(environment.logical_cpus > 0);
    assert!(environment.rustc_version.starts_with("rustc "));
    assert!(environment.cargo_version.starts_with("cargo "));
}

#[test]
fn generated_keys_form_repeatable_prefix_scan_groups() {
    let dataset = Dataset::generate(&smoke_workload()).unwrap();
    let prefix = &dataset.keys[0][..2];
    let matching = dataset
        .keys
        .iter()
        .filter(|key| key.starts_with(prefix))
        .count();

    assert!(matching >= 16, "matching prefix keys: {matching}");
}

#[test]
fn comparison_workloads_reject_parallel_foreground_threads() {
    let mut workload = smoke_workload();
    workload.engine.threads = 2;

    let error = workload.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("engine.threads must be 1 foreground workload thread"),
        "{error}"
    );
}

#[test]
fn compression_equivalence_is_derived_from_the_selected_codec() {
    let none = compression_equivalence(meteordb_bench_support::CompressionConfig::None);
    assert!(
        none.semantic
            .iter()
            .any(|note| note.contains("both engines are configured with no compression"))
    );
    assert!(none.non_equivalent.is_empty());

    let snappy = compression_equivalence(meteordb_bench_support::CompressionConfig::Snappy);
    assert!(snappy.semantic.is_empty());
    assert!(
        snappy
            .non_equivalent
            .iter()
            .any(|note| note.contains("RocksDB uses Snappy"))
    );
}

#[test]
fn component_report_schema_has_tail_latency_size_and_amplification() {
    let report = ComponentBenchmarkReport::new(
        "workloads",
        "inference-cache/zipfian-hit-1kib",
        &[10, 20, 30, 40, 50],
        4096,
        Amplification {
            read: Some(1.25),
            write: None,
            space: None,
            notes: vec!["write and space unavailable".into()],
        },
    )
    .unwrap();

    let value = serde_json::to_value(&report).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["suite"], "workloads");
    assert_eq!(value["benchmark"], "inference-cache/zipfian-hit-1kib");
    assert_eq!(value["latency"]["p95_ns"], 50);
    assert_eq!(value["latency"]["p99_ns"], 50);
    assert_eq!(value["database_bytes"], 4096);
    assert_eq!(value["amplification"]["read"], 1.25);
    assert!(value["amplification"].get("write").unwrap().is_null());
    assert!(value["amplification"].get("space").unwrap().is_null());
}

#[test]
fn prepared_latency_probe_rejects_a_no_op_sample() {
    let mut setups = 0;
    let error = measure_prepared_samples(
        3,
        || {
            setups += 1;
            setups
        },
        |sample| sample,
        |&sample| sample != 2,
    )
    .unwrap_err();

    assert_eq!(setups, 2);
    assert!(error.to_string().contains("sample 2 performed no work"));
}

#[test]
fn prepared_latency_probe_prepares_every_sample_outside_the_operation() {
    let mut setups = 0;
    let latencies = measure_prepared_samples(
        4,
        || {
            setups += 1;
            setups
        },
        |sample| sample,
        |&sample| sample > 0,
    )
    .unwrap();

    assert_eq!(setups, 4);
    assert_eq!(latencies.len(), 4);
}

#[test]
fn unrelated_prior_reads_do_not_change_write_probe_amplification() {
    let clean = read_amplification_delta(
        CounterSnapshot {
            point_reads: 0,
            sstable_probes: 0,
        },
        CounterSnapshot {
            point_reads: 0,
            sstable_probes: 0,
        },
    );
    let after_prior_reads = read_amplification_delta(
        CounterSnapshot {
            point_reads: 500,
            sstable_probes: 750,
        },
        CounterSnapshot {
            point_reads: 500,
            sstable_probes: 750,
        },
    );

    assert_eq!(clean, None);
    assert_eq!(after_prior_reads, clean);
}

#[test]
fn read_amplification_uses_saturating_probe_deltas() {
    let amplification = read_amplification_delta(
        CounterSnapshot {
            point_reads: 10,
            sstable_probes: 20,
        },
        CounterSnapshot {
            point_reads: 14,
            sstable_probes: 18,
        },
    );

    assert_eq!(amplification, Some(0.0));
}
