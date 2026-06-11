# Comparing spatial sort algorithms for well packed GeoParquet

This repository contains a comparison of spatial sort algorithms for well packed GeoParquet files. The algorithms compared are:

- Hilbert curve
- Morton curve
- Sort-Tile-Recursive

## Data

- [pois](https://cogp-demo.spatialty.io/pois.cogp.parquet) (OpenStreetMap)

## Conditions

- Row group size: 10,000 rows
- Compression: ZSTD
- Output file format: GeoParquet v1.1

## Criteria

The algorithms are compared based on the following criteria:
- Sorting time
- Query performance
- File size
- Spatial locality (measured by how small overlap the bounding boxes of each row group are)

## Notes

- Hilbert uses DuckDB spatial's `ST_Hilbert` over the CRS84 world extent.
- Morton uses DuckDB spatial's `ST_QuadKey` at level 23. With a fixed level,
  lexicographic quadkey ordering is Z-order/Morton ordering over the Web
  Mercator tile grid.
- STR pack sorts by bbox center-x, splits rows into approximately square
  strips based on the target row group size, then sorts each strip by center-y
  with alternating direction.

## Results

Full `pois.cogp.parquet` results, run on 2026-06-11 with 30,052,264 rows,
row group size 10,000, 2,935 row groups, 8 DuckDB threads, 3 query warmups,
and 30 measured query repeats.
Query timings in this section use local filesystem reads from `outputs/*.parquet`.
The merged CSV files are in `metrics/full/`.

### Build and size

| Algorithm | Sort time (s) | File size (GiB) | Rows | Row groups |
| --- | ---: | ---: | ---: | ---: |
| Hilbert | 21.1 | 1.888 | 30,052,264 | 2,935 |
| Morton (`ST_QuadKey`) | 24.3 | 1.899 | 30,052,264 | 2,935 |
| STR pack | 47.7 | 1.882 | 30,052,264 | 2,935 |

### Spatial locality

Lower values are better. `sum_area` is the sum of row group bbox areas, and
`overlap_area_ratio` is pairwise overlap area divided by `sum_area`.

| Algorithm | Sum area | Median area | Pairwise overlap area | Overlap area ratio |
| --- | ---: | ---: | ---: | ---: |
| Hilbert | 72,146.0 | 0.494 | 17,197.1 | 0.238 |
| Morton (`ST_QuadKey`) | 155,863.2 | 0.835 | 171,428.5 | 1.100 |
| STR pack | 52,294.3 | 0.451 | 2,813.0 | 0.054 |

Row group bbox overlap:

| Hilbert | Morton (`ST_QuadKey`) | STR pack |
| --- | --- | --- |
| ![Hilbert row group bbox overlap](imgs/hilbert.png) | ![Morton row group bbox overlap](imgs/morton.png) | ![STR pack row group bbox overlap](imgs/str.png) |

Additional views:

| Hilbert | STR pack |
| --- | --- |
| ![Hilbert row group bbox overlap additional view](imgs/hilbert2.png) | ![STR pack row group bbox overlap additional view](imgs/str2.png) |

### Query performance

Median bbox query time in milliseconds. Each query used 3 warmups and 30
measured repeats. The benchmark interleaves algorithms per query with shuffled
order to reduce fixed-order cache bias. Raw timings are in
`metrics/full/query_runs.csv`.

| Algorithm | Tokyo | SF Bay | New York | London | World 1deg |
| --- | ---: | ---: | ---: | ---: | ---: |
| Hilbert | 44.18 | 38.71 | 39.98 | 40.83 | 33.19 |
| Morton (`ST_QuadKey`) | 44.52 | 39.33 | 39.98 | 41.39 | 38.19 |
| STR pack | 44.30 | 37.98 | 40.58 | 41.85 | 37.38 |

P95 bbox query time in milliseconds:

| Algorithm | Tokyo | SF Bay | New York | London | World 1deg |
| --- | ---: | ---: | ---: | ---: | ---: |
| Hilbert | 46.00 | 40.37 | 41.60 | 42.61 | 34.57 |
| Morton (`ST_QuadKey`) | 46.70 | 40.51 | 41.22 | 43.34 | 43.16 |
| STR pack | 46.67 | 39.05 | 42.11 | 43.65 | 40.83 |

To benchmark Parquet files hosted on a remote server, upload the sorted files
with the same names (`pois.hilbert.parquet`, `pois.morton.parquet`,
`pois.str.parquet`) and pass the base URL:

```bash
uv run python scripts/compare_spatial_sort.py bench \
  --bench-output-base https://example.com/path/to/parquets \
  --skip-locality \
  --query-repeats 30 \
  --query-warmups 3 \
  --query-seed 42 \
  --threads 8 \
  --memory-limit 16GB \
  --metrics-dir metrics/remote
```

Remote query metrics include `source=remote` in `queries.csv` and
`query_runs.csv`. Use `--skip-locality` for remote runs unless you explicitly
want to rescan the files to recompute row group bbox metrics.
