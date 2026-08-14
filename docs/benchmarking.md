# Benchmarking and large datasets

The unpublished `csv_ingest_tools` workspace package contains the synthetic
data generator and benchmark runner. These tools are for development and are
not included in the crates.io package.

## Build

```bash
cargo build -p csv_ingest_tools --release --bins
cargo build -p csv_ingest_tools --release --bins --features fast_local
```

Release builds use the repository's native CPU settings and optimized release
profile.

## Generate and parse a dataset

```bash
./target/release/gen --rows 100000000 --with-header \
  | zstd -T0 -q -o data/100m.csv.zst

./target/release/bench \
  --path data/100m.csv.zst \
  --required sku
```

The benchmark accepts:

- `--required <column>`: require a header; repeat for multiple columns;
- `--verify`: validate fields and compute a CRC32;
- `--limit <rows>`: stop after a global row limit;
- `--fast-local`: use the optional mmap parser for a compatible local file.

Examples:

```bash
# gzip and zstd streaming
./target/release/gen --rows 100000000 --with-header \
  | gzip -c > data/100m.csv.gz
./target/release/bench --path data/100m.csv.gz --required sku
./target/release/bench --path data/100m.csv.zst --required sku

# Verify a bounded sample
./target/release/bench \
  --path data/100m.csv.zst \
  --required sku \
  --verify \
  --limit 1000000

# Fast-local path
./target/release/gen --rows 100000000 --with-header > data/100m.csv
./target/release/bench \
  --path data/100m.csv \
  --required sku \
  --fast-local
```

## One billion rows

Generate large fixtures only on storage with sufficient capacity:

```bash
# Uncompressed
./target/release/gen --rows 1000000000 --with-header > data/1b.csv

# gzip
./target/release/gen --rows 1000000000 --with-header \
  | gzip -c > data/1b.csv.gz

# zstd
./target/release/gen --rows 1000000000 --with-header \
  | zstd -T0 -q -o data/1b.csv.zst
```

Sanity-check the uncompressed fixture and compare parser paths with bounded
verification:

```bash
wc -l data/1b.csv
./target/release/bench \
  --path data/1b.csv \
  --required sku \
  --fast-local \
  --verify \
  --limit 1000000
```

The line count should be `1,000,000,001`, including the header.

## Interpreting results

- Compression or storage can dominate end-to-end throughput. Use an
  uncompressed local file to isolate parser speed.
- Gzip is normally slower to decode than zstd or uncompressed input.
- With flexible rows and verification disabled, placing required columns early
  lets fast-local stop scanning after the last required field.
- Keep network download time separate when measuring S3 or other remote input.
