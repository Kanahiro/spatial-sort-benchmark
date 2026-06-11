#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import duckdb


ALGORITHMS = ("hilbert", "morton", "str")
WORLD_XMIN = -180.0
WORLD_YMIN = -90.0
WORLD_XMAX = 180.0
WORLD_YMAX = 90.0
QUADKEY_LEVEL = 23

DEFAULT_QUERIES = (
    ("tokyo", 139.55, 35.50, 139.95, 35.85),
    ("sf_bay", -122.65, 37.55, -121.75, 38.05),
    ("new_york", -74.30, 40.45, -73.65, 40.95),
    ("london", -0.55, 51.25, 0.35, 51.75),
    ("world_1deg", -0.50, -0.50, 0.50, 0.50),
)


@dataclass
class BuildResult:
    algorithm: str
    output: str
    seconds: float | None
    bytes: int
    row_groups: int
    rows: int
    row_group_size: int
    skipped: bool


@dataclass
class LocalityMetrics:
    algorithm: str
    output: str
    row_groups: int
    sum_area: float
    avg_area: float
    median_area: float
    max_area: float
    pairwise_overlap_area: float
    avg_pairwise_overlap_area: float
    overlap_area_ratio: float


@dataclass
class QueryMetrics:
    algorithm: str
    source: str
    query: str
    bbox: tuple[float, float, float, float]
    rows: int
    repeats: int
    warmups: int
    best_seconds: float
    mean_seconds: float
    median_seconds: float
    p25_seconds: float
    p75_seconds: float
    p95_seconds: float
    stdev_seconds: float
    all_seconds: list[float]


def sql_literal_path(path: str | Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def is_remote_ref(ref: str) -> bool:
    return ref.startswith(("http://", "https://", "s3://", "gs://", "r2://"))


def load_extension(con: duckdb.DuckDBPyConnection, name: str) -> None:
    try:
        con.execute(f"LOAD {name}")
    except duckdb.IOException:
        con.execute(f"INSTALL {name}")
        con.execute(f"LOAD {name}")


def connect(threads: int | None, memory_limit: str | None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if threads is not None:
        con.execute(f"SET threads = {int(threads)}")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    load_extension(con, "spatial")
    return con


def source_scan(input_path: Path, limit: int | None) -> str:
    scan = f"read_parquet({sql_literal_path(input_path)})"
    if limit is None:
        return f"SELECT * FROM {scan}"
    return f"SELECT * FROM {scan} LIMIT {int(limit)}"


def sorted_select_sql(
    algorithm: str,
    input_path: Path,
    limit: int | None,
    row_group_size: int,
    con: duckdb.DuckDBPyConnection,
) -> str:
    base = source_scan(input_path, limit)
    if algorithm == "hilbert":
        return f"""
            WITH src AS (
                SELECT
                    *,
                    (bbox.xmin + bbox.xmax) / 2.0 AS __cx,
                    (bbox.ymin + bbox.ymax) / 2.0 AS __cy
                FROM ({base})
            )
            SELECT id, tags, geometry, bbox
            FROM src
            ORDER BY
                ST_Hilbert(
                    __cx,
                    __cy,
                    ST_MakeBox2D(
                        ST_Point({WORLD_XMIN}, {WORLD_YMIN}),
                        ST_Point({WORLD_XMAX}, {WORLD_YMAX})
                    )
                ),
                id
        """
    if algorithm == "morton":
        return f"""
            WITH src AS (
                SELECT
                    *,
                    (bbox.xmin + bbox.xmax) / 2.0 AS __cx,
                    (bbox.ymin + bbox.ymax) / 2.0 AS __cy
                FROM ({base})
            )
            SELECT id, tags, geometry, bbox
            FROM src
            ORDER BY ST_QuadKey(__cx, __cy, {QUADKEY_LEVEL}), id
        """
    if algorithm == "str":
        rows = con.execute(f"SELECT count(*) FROM ({base})").fetchone()[0]
        tile_count = max(1, math.ceil(rows / row_group_size))
        strip_count = max(1, math.ceil(math.sqrt(tile_count)))
        strip_size = math.ceil(rows / strip_count)
        return f"""
            WITH src AS (
                SELECT
                    *,
                    (bbox.xmin + bbox.xmax) / 2.0 AS __cx,
                    (bbox.ymin + bbox.ymax) / 2.0 AS __cy
                FROM ({base})
            ),
            x_ranked AS (
                SELECT
                    *,
                    floor((row_number() OVER (ORDER BY __cx, __cy, id) - 1) / {strip_size})::BIGINT AS __strip
                FROM src
            )
            SELECT id, tags, geometry, bbox
            FROM x_ranked
            ORDER BY
                __strip,
                CASE WHEN __strip % 2 = 0 THEN __cy ELSE -__cy END,
                __cx,
                id
        """
    raise ValueError(f"unknown algorithm: {algorithm}")


def build_one(
    con: duckdb.DuckDBPyConnection,
    algorithm: str,
    input_path: Path,
    output_dir: Path,
    row_group_size: int,
    limit: int | None,
    force: bool,
) -> BuildResult:
    suffix = f".limit{limit}" if limit is not None else ""
    output_path = output_dir / f"pois.{algorithm}{suffix}.parquet"
    if output_path.exists() and not force:
        elapsed = None
        skipped = True
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            output_path.unlink()
        query = sorted_select_sql(algorithm, input_path, limit, row_group_size, con)
        copy_sql = f"""
            COPY ({query})
            TO {sql_literal_path(output_path)}
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {int(row_group_size)})
        """
        start = time.perf_counter()
        con.execute(copy_sql)
        elapsed = time.perf_counter() - start
        skipped = False

    rows = con.execute(
        f"SELECT count(*) FROM read_parquet({sql_literal_path(output_path)})"
    ).fetchone()[0]
    row_groups = con.execute(
        f"""
        SELECT count(*)
        FROM parquet_metadata({sql_literal_path(output_path)})
        WHERE path_in_schema = 'id'
        """
    ).fetchone()[0]
    return BuildResult(
        algorithm=algorithm,
        output=str(output_path),
        seconds=elapsed,
        bytes=output_path.stat().st_size,
        row_groups=row_groups,
        rows=rows,
        row_group_size=row_group_size,
        skipped=skipped,
    )


def build(
    con: duckdb.DuckDBPyConnection,
    algorithms: Iterable[str],
    input_path: Path,
    output_dir: Path,
    row_group_size: int,
    limit: int | None,
    force: bool,
) -> list[BuildResult]:
    return [
        build_one(con, algorithm, input_path, output_dir, row_group_size, limit, force)
        for algorithm in algorithms
    ]


def row_group_sizes(con: duckdb.DuckDBPyConnection, path: Path) -> list[int]:
    return [
        int(row[0])
        for row in con.execute(
            f"""
            SELECT row_group_num_rows
            FROM parquet_metadata({sql_literal_path(path)})
            WHERE path_in_schema = 'id'
            ORDER BY row_group_id
            """
        ).fetchall()
    ]


def create_row_group_bbox_table(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    table_name: str,
) -> None:
    sizes = row_group_sizes(con, path)
    if not sizes:
        raise ValueError(f"no row groups found: {path}")
    starts: list[tuple[int, int, int]] = []
    offset = 0
    for row_group_id, size in enumerate(sizes):
        starts.append((row_group_id, offset, offset + size))
        offset += size

    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute("CREATE OR REPLACE TEMP TABLE __row_group_ranges(row_group_id INTEGER, start_row BIGINT, end_row BIGINT)")
    con.executemany("INSERT INTO __row_group_ranges VALUES (?, ?, ?)", starts)
    con.execute(
        f"""
        CREATE TEMP TABLE {table_name} AS
        SELECT
            r.row_group_id,
            min(p.bbox.xmin) AS xmin,
            min(p.bbox.ymin) AS ymin,
            max(p.bbox.xmax) AS xmax,
            max(p.bbox.ymax) AS ymax,
            count(*) AS rows,
            greatest(0.0, max(p.bbox.xmax) - min(p.bbox.xmin)) *
            greatest(0.0, max(p.bbox.ymax) - min(p.bbox.ymin)) AS area
        FROM read_parquet({sql_literal_path(path)}, file_row_number = true) AS p
        JOIN __row_group_ranges AS r
          ON p.file_row_number >= r.start_row
         AND p.file_row_number < r.end_row
        GROUP BY r.row_group_id
        ORDER BY r.row_group_id
        """
    )
    con.execute("DROP TABLE __row_group_ranges")


def locality(con: duckdb.DuckDBPyConnection, algorithm: str, path: Path) -> LocalityMetrics:
    table_name = f"__bbox_{algorithm}"
    create_row_group_bbox_table(con, path, table_name)
    row_groups, sum_area, avg_area, median_area, max_area = con.execute(
        f"""
        SELECT
            count(*),
            coalesce(sum(area), 0.0),
            coalesce(avg(area), 0.0),
            coalesce(median(area), 0.0),
            coalesce(max(area), 0.0)
        FROM {table_name}
        """
    ).fetchone()
    pairwise_overlap_area = con.execute(
        f"""
        SELECT coalesce(sum(
            greatest(0.0, least(a.xmax, b.xmax) - greatest(a.xmin, b.xmin)) *
            greatest(0.0, least(a.ymax, b.ymax) - greatest(a.ymin, b.ymin))
        ), 0.0)
        FROM {table_name} AS a
        JOIN {table_name} AS b
          ON a.row_group_id < b.row_group_id
         AND a.xmin <= b.xmax
         AND a.xmax >= b.xmin
         AND a.ymin <= b.ymax
         AND a.ymax >= b.ymin
        """
    ).fetchone()[0]
    pairs = row_groups * (row_groups - 1) / 2
    return LocalityMetrics(
        algorithm=algorithm,
        output=str(path),
        row_groups=int(row_groups),
        sum_area=float(sum_area),
        avg_area=float(avg_area),
        median_area=float(median_area),
        max_area=float(max_area),
        pairwise_overlap_area=float(pairwise_overlap_area),
        avg_pairwise_overlap_area=float(pairwise_overlap_area / pairs if pairs else 0.0),
        overlap_area_ratio=float(pairwise_overlap_area / sum_area if sum_area else 0.0),
    )


def percentile(values: list[float], p: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    index = (len(ordered) - 1) * p
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def bbox_query_sql(path: str, bbox: tuple[float, float, float, float]) -> str:
    xmin, ymin, xmax, ymax = bbox
    return f"""
        SELECT count(*)
        FROM read_parquet({sql_literal_path(path)})
        WHERE bbox.xmax >= {xmin}
          AND bbox.xmin <= {xmax}
          AND bbox.ymax >= {ymin}
          AND bbox.ymin <= {ymax}
    """


def query_metrics(
    algorithm: str,
    source: str,
    query_name: str,
    bbox: tuple[float, float, float, float],
    rows: int,
    repeats: int,
    warmups: int,
    times: list[float],
) -> QueryMetrics:
    return QueryMetrics(
        algorithm=algorithm,
        source=source,
        query=query_name,
        bbox=bbox,
        rows=rows,
        repeats=repeats,
        warmups=warmups,
        best_seconds=min(times),
        mean_seconds=statistics.fmean(times),
        median_seconds=statistics.median(times),
        p25_seconds=percentile(times, 0.25),
        p75_seconds=percentile(times, 0.75),
        p95_seconds=percentile(times, 0.95),
        stdev_seconds=statistics.stdev(times) if len(times) > 1 else 0.0,
        all_seconds=times,
    )


def benchmark_queries_interleaved(
    con: duckdb.DuckDBPyConnection,
    outputs: dict[str, str],
    source: str,
    queries: Iterable[tuple[str, float, float, float, float]],
    repeats: int,
    warmups: int,
    seed: int,
) -> list[QueryMetrics]:
    results: list[QueryMetrics] = []
    rng = random.Random(seed)
    algorithms = list(outputs.keys())

    for query_index, (name, xmin, ymin, xmax, ymax) in enumerate(queries):
        bbox = (xmin, ymin, xmax, ymax)
        sql_by_algorithm = {
            algorithm: bbox_query_sql(path, bbox)
            for algorithm, path in outputs.items()
        }
        rows_by_algorithm: dict[str, int] = {}
        times_by_algorithm: dict[str, list[float]] = {
            algorithm: []
            for algorithm in algorithms
        }

        for warmup_index in range(warmups):
            order = algorithms[:]
            rng.shuffle(order)
            for algorithm in order:
                rows_by_algorithm[algorithm] = int(
                    con.execute(sql_by_algorithm[algorithm]).fetchone()[0]
                )

        for repeat_index in range(repeats):
            order = algorithms[:]
            rng.shuffle(order)
            for algorithm in order:
                start = time.perf_counter_ns()
                rows = int(con.execute(sql_by_algorithm[algorithm]).fetchone()[0])
                elapsed = (time.perf_counter_ns() - start) / 1_000_000_000
                rows_by_algorithm[algorithm] = rows
                times_by_algorithm[algorithm].append(elapsed)

        for algorithm in algorithms:
            results.append(
                query_metrics(
                    algorithm,
                    source,
                    name,
                    bbox,
                    rows_by_algorithm[algorithm],
                    repeats,
                    warmups,
                    times_by_algorithm[algorithm],
                )
            )

    return results


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_build_csv(path: Path, results: list[BuildResult]) -> None:
    write_csv(
        path,
        [
            {
                "algorithm": result.algorithm,
                "seconds": "" if result.seconds is None else result.seconds,
                "skipped": result.skipped,
                "rows": result.rows,
                "row_groups": result.row_groups,
                "row_group_size": result.row_group_size,
                "bytes": result.bytes,
                "output": result.output,
            }
            for result in results
        ],
        [
            "algorithm",
            "seconds",
            "skipped",
            "rows",
            "row_groups",
            "row_group_size",
            "bytes",
            "output",
        ],
    )


def write_locality_csv(path: Path, results: list[LocalityMetrics]) -> None:
    write_csv(
        path,
        [
            {
                "algorithm": result.algorithm,
                "output": result.output,
                "row_groups": result.row_groups,
                "sum_area": result.sum_area,
                "avg_area": result.avg_area,
                "median_area": result.median_area,
                "max_area": result.max_area,
                "pairwise_overlap_area": result.pairwise_overlap_area,
                "avg_pairwise_overlap_area": result.avg_pairwise_overlap_area,
                "overlap_area_ratio": result.overlap_area_ratio,
            }
            for result in results
        ],
        [
            "algorithm",
            "output",
            "row_groups",
            "sum_area",
            "avg_area",
            "median_area",
            "max_area",
            "pairwise_overlap_area",
            "avg_pairwise_overlap_area",
            "overlap_area_ratio",
        ],
    )


def write_query_csv(path: Path, results: list[QueryMetrics]) -> None:
    write_csv(
        path,
        [
            {
                "algorithm": result.algorithm,
                "source": result.source,
                "query": result.query,
                "xmin": result.bbox[0],
                "ymin": result.bbox[1],
                "xmax": result.bbox[2],
                "ymax": result.bbox[3],
                "rows": result.rows,
                "repeats": result.repeats,
                "warmups": result.warmups,
                "best_seconds": result.best_seconds,
                "mean_seconds": result.mean_seconds,
                "median_seconds": result.median_seconds,
                "p25_seconds": result.p25_seconds,
                "p75_seconds": result.p75_seconds,
                "p95_seconds": result.p95_seconds,
                "stdev_seconds": result.stdev_seconds,
            }
            for result in results
        ],
        [
            "algorithm",
            "source",
            "query",
            "xmin",
            "ymin",
            "xmax",
            "ymax",
            "rows",
            "repeats",
            "warmups",
            "best_seconds",
            "mean_seconds",
            "median_seconds",
            "p25_seconds",
            "p75_seconds",
            "p95_seconds",
            "stdev_seconds",
        ],
    )


def write_query_runs_csv(path: Path, results: list[QueryMetrics]) -> None:
    rows: list[dict[str, object]] = []
    for result in results:
        for index, seconds in enumerate(result.all_seconds, start=1):
            rows.append(
                {
                    "algorithm": result.algorithm,
                    "source": result.source,
                    "query": result.query,
                    "run": index,
                    "seconds": seconds,
                    "rows": result.rows,
                    "xmin": result.bbox[0],
                    "ymin": result.bbox[1],
                    "xmax": result.bbox[2],
                    "ymax": result.bbox[3],
                }
            )
    write_csv(
        path,
        rows,
        [
            "algorithm",
            "source",
            "query",
            "run",
            "seconds",
            "rows",
            "xmin",
            "ymin",
            "xmax",
            "ymax",
        ],
    )


def print_build_results(results: list[BuildResult]) -> None:
    print("algorithm,seconds,skipped,rows,row_groups,bytes,output")
    for result in results:
        seconds = "" if result.seconds is None else f"{result.seconds:.3f}"
        print(
            f"{result.algorithm},{seconds},{result.skipped},{result.rows},"
            f"{result.row_groups},{result.bytes},{result.output}"
        )


def print_locality_results(results: list[LocalityMetrics]) -> None:
    print(
        "algorithm,row_groups,sum_area,avg_area,median_area,max_area,"
        "pairwise_overlap_area,avg_pairwise_overlap_area,overlap_area_ratio"
    )
    for result in results:
        print(
            f"{result.algorithm},{result.row_groups},{result.sum_area:.9f},"
            f"{result.avg_area:.9f},{result.median_area:.9f},{result.max_area:.9f},"
            f"{result.pairwise_overlap_area:.9f},"
            f"{result.avg_pairwise_overlap_area:.9f},"
            f"{result.overlap_area_ratio:.9f}"
        )


def print_query_results(results: list[QueryMetrics]) -> None:
    print(
        "algorithm,source,query,rows,repeats,warmups,best_seconds,mean_seconds,"
        "median_seconds,p95_seconds,stdev_seconds,bbox"
    )
    for result in results:
        print(
            f"{result.algorithm},{result.source},{result.query},{result.rows},{result.repeats},"
            f"{result.warmups},{result.best_seconds:.6f},{result.mean_seconds:.6f},"
            f"{result.median_seconds:.6f},{result.p95_seconds:.6f},"
            f"{result.stdev_seconds:.6f},"
            f"{result.bbox}"
        )


def parse_algorithms(value: str) -> list[str]:
    algorithms = [item.strip().lower() for item in value.split(",") if item.strip()]
    unknown = sorted(set(algorithms) - set(ALGORITHMS))
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown algorithm(s): {', '.join(unknown)}")
    return algorithms


def output_name(algorithm: str, limit: int | None) -> str:
    suffix = f".limit{limit}" if limit is not None else ""
    return f"pois.{algorithm}{suffix}.parquet"


def existing_outputs(
    output_dir: Path,
    algorithms: Iterable[str],
    limit: int | None,
    bench_output_base: str | None,
) -> dict[str, str]:
    if bench_output_base:
        base = bench_output_base.rstrip("/")
        return {
            algorithm: f"{base}/{output_name(algorithm, limit)}"
            for algorithm in algorithms
        }
    return {
        algorithm: str(output_dir / output_name(algorithm, limit))
        for algorithm in algorithms
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare Hilbert, Morton, and STR packed GeoParquet sorting."
    )
    parser.add_argument("command", choices=("build", "bench", "all"))
    parser.add_argument("--input", type=Path, default=Path("pois.cogp.parquet"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    parser.add_argument("--metrics-dir", type=Path, default=Path("metrics"))
    parser.add_argument("--algorithms", type=parse_algorithms, default=list(ALGORITHMS))
    parser.add_argument("--row-group-size", type=int, default=10_000)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--threads", type=int, default=None)
    parser.add_argument("--memory-limit", default=None)
    parser.add_argument("--query-repeats", type=int, default=30)
    parser.add_argument("--query-warmups", type=int, default=3)
    parser.add_argument("--query-seed", type=int, default=0)
    parser.add_argument(
        "--bench-output-base",
        default=None,
        help=(
            "Read benchmark Parquet files from this base path/URL instead of "
            "--output-dir. The script appends pois.<algorithm>.parquet."
        ),
    )
    parser.add_argument(
        "--skip-locality",
        action="store_true",
        help="Skip row group bbox locality metrics and run only query benchmarks.",
    )
    args = parser.parse_args()

    con = connect(args.threads, args.memory_limit)
    build_results: list[BuildResult] = []

    if args.command in ("build", "all"):
        build_results = build(
            con,
            args.algorithms,
            args.input,
            args.output_dir,
            args.row_group_size,
            args.limit,
            args.force,
        )
        print_build_results(build_results)
        write_build_csv(args.metrics_dir / "build.csv", build_results)

    if args.command in ("bench", "all"):
        outputs = existing_outputs(
            args.output_dir,
            args.algorithms,
            args.limit,
            args.bench_output_base,
        )
        has_remote_output = any(is_remote_ref(ref) for ref in outputs.values())
        if has_remote_output:
            load_extension(con, "httpfs")

        missing = [
            ref
            for ref in outputs.values()
            if not is_remote_ref(ref) and not Path(ref).exists()
        ]
        if missing:
            raise SystemExit("missing output files; run build first: " + ", ".join(missing))

        if not args.skip_locality:
            locality_results = [
                locality(con, algorithm, path)
                for algorithm, path in outputs.items()
            ]
            print_locality_results(locality_results)
            write_locality_csv(args.metrics_dir / "locality.csv", locality_results)

        query_results = benchmark_queries_interleaved(
            con,
            outputs,
            "remote" if has_remote_output else "local",
            DEFAULT_QUERIES,
            args.query_repeats,
            args.query_warmups,
            args.query_seed,
        )
        print_query_results(query_results)
        write_query_csv(args.metrics_dir / "queries.csv", query_results)
        write_query_runs_csv(args.metrics_dir / "query_runs.csv", query_results)


if __name__ == "__main__":
    main()
