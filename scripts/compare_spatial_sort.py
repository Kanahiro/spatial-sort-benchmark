#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import random
import statistics
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import duckdb


ALGORITHMS = ("hilbert", "morton", "str", "boost_pack")
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
    ("paris", 2.20, 48.75, 2.45, 48.95),
    ("singapore", 103.60, 1.20, 104.05, 1.50),
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
    candidate_row_groups: int
    total_row_groups: int
    candidate_row_group_fraction: float
    candidate_rows: int
    total_rows: int
    candidate_row_fraction: float
    repeats: int
    warmups: int
    best_seconds: float
    mean_seconds: float
    median_seconds: float
    p25_seconds: float
    p75_seconds: float
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


def disable_duckdb_caches(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET enable_external_file_cache = false")
    con.execute("SET enable_http_metadata_cache = false")
    con.execute("SET parquet_metadata_cache = false")
    con.execute("SET enable_caching_operators = false")


def connect(
    threads: int | None,
    memory_limit: str | None,
    *,
    disable_cache: bool = False,
    load_spatial: bool = True,
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if threads is not None:
        con.execute(f"SET threads = {int(threads)}")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    if disable_cache:
        disable_duckdb_caches(con)
    if load_spatial:
        load_extension(con, "spatial")
    return con


def source_scan(input_path: Path, limit: int | None) -> str:
    scan = f"read_parquet({sql_literal_path(input_path)})"
    if limit is None:
        return f"SELECT * FROM {scan}"
    scan = f"read_parquet({sql_literal_path(input_path)}, file_row_number = true)"
    return f"SELECT id, tags, geometry, bbox FROM {scan} WHERE file_row_number < {int(limit)}"


def actual_row_group_size(con: duckdb.DuckDBPyConnection, requested: int) -> int:
    # DuckDB rounds ROW_GROUP_SIZE to a writer granularity. Packing leaves must
    # match the written row groups, or a group straddles two unrelated leaves.
    with tempfile.TemporaryDirectory(prefix="row_group_probe_") as temp_dir:
        probe = Path(temp_dir) / "probe.parquet"
        con.execute(
            f"""
            COPY (SELECT i FROM range({2 * requested}) AS t(i))
            TO {sql_literal_path(probe)}
            (FORMAT PARQUET, ROW_GROUP_SIZE {requested})
            """
        )
        return int(
            con.execute(
                f"""
                SELECT row_group_num_rows
                FROM parquet_metadata({sql_literal_path(probe)})
                WHERE path_in_schema = 'i'
                ORDER BY row_group_id
                LIMIT 1
                """
            ).fetchone()[0]
        )


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
        # A strip must contain whole Parquet leaves; otherwise one row group
        # spans two strips and its bbox can overlap unrelated leaves.
        strip_size = math.ceil(tile_count / strip_count) * row_group_size
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


def boost_pack_select_sql(
    con: duckdb.DuckDBPyConnection,
    input_path: Path,
    node_capacity: int,
    limit: int | None,
) -> str:
    """Materialize Boost-style leaf order in DuckDB and return its final scan."""
    source = f"read_parquet({sql_literal_path(input_path)}, file_row_number = true)"
    limit_filter = f"WHERE file_row_number < {int(limit)}" if limit is not None else ""
    rows, xmin, ymin, xmax, ymax, invalid = con.execute(
        f"""
        SELECT count(*), min(bbox.xmin), min(bbox.ymin),
               max(bbox.xmax), max(bbox.ymax),
               count(*) FILTER (WHERE
                   NOT (isfinite(bbox.xmin) AND isfinite(bbox.ymin)
                        AND isfinite(bbox.xmax) AND isfinite(bbox.ymax)
                        AND bbox.xmin <= bbox.xmax
                        AND bbox.ymin <= bbox.ymax)
                   OR bbox.xmin IS NULL OR bbox.ymin IS NULL
                   OR bbox.xmax IS NULL OR bbox.ymax IS NULL)
        FROM {source} {limit_filter}
        """
    ).fetchone()
    if not rows:
        raise ValueError("Boost packing requires at least one input row")
    if invalid:
        raise ValueError(f"Boost packing requires valid finite bboxes ({invalid} invalid)")
    if node_capacity < 2:
        raise ValueError("Boost packing requires node_capacity >= 2")

    # Boost's initial subtree capacity is the largest power of Max below N.
    subtree_max = 1
    power = node_capacity
    while power < rows:
        subtree_max = power
        power *= node_capacity
    subtree_min = max(1, node_capacity * 3 // 10) * (subtree_max // node_capacity)

    con.execute("DROP TABLE IF EXISTS __boost_pack_rows")
    con.execute(
        f"""
        CREATE TEMP TABLE __boost_pack_rows AS
        SELECT file_row_number,
               (bbox.xmin + bbox.xmax) / 2 AS cx,
               (bbox.ymin + bbox.ymax) / 2 AS cy,
               0::BIGINT AS start_rank,
               file_row_number::BIGINT AS local_rank,
               {xmin}::DOUBLE AS hint_xmin,
               {ymin}::DOUBLE AS hint_ymin,
               {xmax}::DOUBLE AS hint_xmax,
               {ymax}::DOUBLE AS hint_ymax,
               {subtree_max}::BIGINT AS maxc,
               {subtree_min}::BIGINT AS minc
        FROM {source} {limit_filter}
        """
    )

    # Each pass handles all current nodes. SQL window ranks implement the
    # median split; the node bounds remain the geometric half-box used by Boost.
    while con.execute("SELECT max(maxc) FROM __boost_pack_rows").fetchone()[0] > 1:
        con.execute("DROP TABLE IF EXISTS __boost_pack_next")
        con.execute(
            f"""
            CREATE TEMP TABLE __boost_pack_next AS
            WITH groups AS (
                SELECT start_rank, count(*) AS n,
                       min(hint_xmin) AS xmin, min(hint_ymin) AS ymin,
                       min(hint_xmax) AS xmax, min(hint_ymax) AS ymax,
                       min(maxc) AS maxc, min(minc) AS minc
                FROM __boost_pack_rows
                GROUP BY start_rank
            ),
            counts AS (
                SELECT *, n // maxc AS q, n % maxc AS rem,
                       (n - minc) // maxc AS q2,
                       (n - minc) % maxc AS rem2
                FROM groups
            ),
            plans AS (
                SELECT *,
                    CASE
                        WHEN rem = 0 THEN (q // 2) * maxc
                        WHEN rem >= minc THEN ((q + 1) // 2) * maxc
                        WHEN rem2 = 0 THEN ((q2 + 1) // 2) * maxc
                        WHEN q2 = 0 THEN rem2
                        ELSE ((q2 + 2) // 2) * maxc
                    END AS median_count,
                    (xmax - xmin >= ymax - ymin) AS split_x,
                    (xmin + (xmax - xmin) / 2) AS xmid,
                    (ymin + (ymax - ymin) / 2) AS ymid
                FROM counts
            ),
            ranked AS (
                SELECT r.file_row_number, r.cx, r.cy, p.*,
                       row_number() OVER (
                           PARTITION BY r.start_rank
                           ORDER BY CASE WHEN p.split_x THEN r.cx ELSE r.cy END,
                                    r.file_row_number
                       ) AS rn
                FROM __boost_pack_rows AS r
                JOIN plans AS p USING (start_rank)
                WHERE p.maxc > 1 AND p.n > p.maxc
            )
            SELECT file_row_number, cx, cy,
                   CASE WHEN rn <= median_count THEN start_rank
                        ELSE start_rank + median_count END AS start_rank,
                   CASE WHEN rn <= median_count THEN rn - 1
                        ELSE rn - median_count - 1 END AS local_rank,
                   CASE WHEN split_x AND rn > median_count THEN xmid
                        ELSE xmin END AS hint_xmin,
                   CASE WHEN NOT split_x AND rn > median_count THEN ymid
                        ELSE ymin END AS hint_ymin,
                   CASE WHEN split_x AND rn <= median_count THEN xmid
                        ELSE xmax END AS hint_xmax,
                   CASE WHEN NOT split_x AND rn <= median_count THEN ymid
                        ELSE ymax END AS hint_ymax,
                   maxc, minc
            FROM ranked
            UNION ALL
            SELECT r.file_row_number, r.cx, r.cy, r.start_rank, r.local_rank,
                   r.hint_xmin, r.hint_ymin, r.hint_xmax, r.hint_ymax,
                   CASE WHEN p.maxc > 1 THEN p.maxc // {node_capacity}
                        ELSE p.maxc END AS maxc,
                   CASE WHEN p.maxc > 1 THEN p.minc // {node_capacity}
                        ELSE p.minc END AS minc
            FROM __boost_pack_rows AS r
            JOIN plans AS p USING (start_rank)
            WHERE p.maxc <= 1 OR p.n <= p.maxc
            """
        )
        con.execute("DROP TABLE __boost_pack_rows")
        con.execute("ALTER TABLE __boost_pack_next RENAME TO __boost_pack_rows")

    return f"""
        SELECT p.id, p.tags, p.geometry, p.bbox
        FROM {source} AS p
        JOIN __boost_pack_rows AS packed USING (file_row_number)
        ORDER BY packed.start_rank, packed.local_rank
    """


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
        start = time.perf_counter()
        if algorithm == "boost_pack":
            node_capacity = actual_row_group_size(con, row_group_size)
            query = boost_pack_select_sql(con, input_path, node_capacity, limit)
            con.execute(
                f"""
                COPY ({query}) TO {sql_literal_path(output_path)}
                (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {int(row_group_size)})
                """
            )
        else:
            leaf_size = (
                actual_row_group_size(con, row_group_size)
                if algorithm == "str"
                else row_group_size
            )
            query = sorted_select_sql(algorithm, input_path, limit, leaf_size, con)
            copy_sql = f"""
                COPY ({query})
                TO {sql_literal_path(output_path)}
                (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {int(row_group_size)})
            """
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


def row_group_candidates(
    con: duckdb.DuckDBPyConnection,
    path: str,
    bbox: tuple[float, float, float, float],
) -> tuple[int, int, int, int]:
    xmin, ymin, xmax, ymax = bbox
    candidate_row_groups, total_row_groups, candidate_rows, total_rows = con.execute(
        f"""
        WITH row_groups AS (
            SELECT
                row_group_id,
                max(row_group_num_rows) AS rows,
                min(CASE
                    WHEN path_in_schema = 'bbox, xmin'
                    THEN try_cast(stats_min_value AS DOUBLE)
                END) AS xmin,
                min(CASE
                    WHEN path_in_schema = 'bbox, ymin'
                    THEN try_cast(stats_min_value AS DOUBLE)
                END) AS ymin,
                max(CASE
                    WHEN path_in_schema = 'bbox, xmax'
                    THEN try_cast(stats_max_value AS DOUBLE)
                END) AS xmax,
                max(CASE
                    WHEN path_in_schema = 'bbox, ymax'
                    THEN try_cast(stats_max_value AS DOUBLE)
                END) AS ymax
            FROM parquet_metadata({sql_literal_path(path)})
            WHERE path_in_schema IN ('bbox, xmin', 'bbox, ymin', 'bbox, xmax', 'bbox, ymax')
            GROUP BY row_group_id
        ),
        candidates AS (
            SELECT *
            FROM row_groups
            WHERE xmax >= {xmin}
              AND xmin <= {xmax}
              AND ymax >= {ymin}
              AND ymin <= {ymax}
        )
        SELECT
            (SELECT count(*) FROM candidates),
            (SELECT count(*) FROM row_groups),
            coalesce((SELECT sum(rows) FROM candidates), 0),
            coalesce((SELECT sum(rows) FROM row_groups), 0)
        """
    ).fetchone()
    return (
        int(candidate_row_groups),
        int(total_row_groups),
        int(candidate_rows),
        int(total_rows),
    )


def query_metrics(
    algorithm: str,
    source: str,
    query_name: str,
    bbox: tuple[float, float, float, float],
    rows: int,
    candidate_row_groups: int,
    total_row_groups: int,
    candidate_rows: int,
    total_rows: int,
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
        candidate_row_groups=candidate_row_groups,
        total_row_groups=total_row_groups,
        candidate_row_group_fraction=(
            candidate_row_groups / total_row_groups
            if total_row_groups
            else 0.0
        ),
        candidate_rows=candidate_rows,
        total_rows=total_rows,
        candidate_row_fraction=candidate_rows / total_rows if total_rows else 0.0,
        repeats=repeats,
        warmups=warmups,
        best_seconds=min(times),
        mean_seconds=statistics.fmean(times),
        median_seconds=statistics.median(times),
        p25_seconds=percentile(times, 0.25),
        p75_seconds=percentile(times, 0.75),
        stdev_seconds=statistics.stdev(times) if len(times) > 1 else 0.0,
        all_seconds=times,
    )


def execute_count_query(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    *,
    fresh_connection: bool,
    threads: int | None,
    memory_limit: str | None,
    needs_httpfs: bool,
) -> int:
    if not fresh_connection:
        return int(con.execute(sql).fetchone()[0])

    query_con = connect(
        threads,
        memory_limit,
        disable_cache=True,
        load_spatial=False,
    )
    if needs_httpfs:
        load_extension(query_con, "httpfs")
    try:
        return int(query_con.execute(sql).fetchone()[0])
    finally:
        query_con.close()


def benchmark_queries_interleaved(
    con: duckdb.DuckDBPyConnection,
    outputs: dict[str, str],
    source: str,
    queries: Iterable[tuple[str, float, float, float, float]],
    repeats: int,
    warmups: int,
    seed: int,
    fresh_connection_per_query: bool,
    threads: int | None,
    memory_limit: str | None,
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
        row_group_counts = {
            algorithm: row_group_candidates(con, path, bbox)
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
                rows_by_algorithm[algorithm] = execute_count_query(
                    con,
                    sql_by_algorithm[algorithm],
                    fresh_connection=fresh_connection_per_query,
                    threads=threads,
                    memory_limit=memory_limit,
                    needs_httpfs=source == "remote",
                )

        for repeat_index in range(repeats):
            order = algorithms[:]
            rng.shuffle(order)
            for algorithm in order:
                if fresh_connection_per_query:
                    query_con = connect(
                        threads,
                        memory_limit,
                        disable_cache=True,
                        load_spatial=False,
                    )
                    if source == "remote":
                        load_extension(query_con, "httpfs")
                    try:
                        start = time.perf_counter_ns()
                        rows = int(query_con.execute(sql_by_algorithm[algorithm]).fetchone()[0])
                        elapsed = (time.perf_counter_ns() - start) / 1_000_000_000
                    finally:
                        query_con.close()
                else:
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
                    row_group_counts[algorithm][0],
                    row_group_counts[algorithm][1],
                    row_group_counts[algorithm][2],
                    row_group_counts[algorithm][3],
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
                "candidate_row_groups": result.candidate_row_groups,
                "total_row_groups": result.total_row_groups,
                "candidate_row_group_fraction": result.candidate_row_group_fraction,
                "candidate_rows": result.candidate_rows,
                "total_rows": result.total_rows,
                "candidate_row_fraction": result.candidate_row_fraction,
                "repeats": result.repeats,
                "warmups": result.warmups,
                "best_seconds": result.best_seconds,
                "mean_seconds": result.mean_seconds,
                "median_seconds": result.median_seconds,
                "p25_seconds": result.p25_seconds,
                "p75_seconds": result.p75_seconds,
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
            "candidate_row_groups",
            "total_row_groups",
            "candidate_row_group_fraction",
            "candidate_rows",
            "total_rows",
            "candidate_row_fraction",
            "repeats",
            "warmups",
            "best_seconds",
            "mean_seconds",
            "median_seconds",
            "p25_seconds",
            "p75_seconds",
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
                    "candidate_row_groups": result.candidate_row_groups,
                    "total_row_groups": result.total_row_groups,
                    "candidate_row_group_fraction": result.candidate_row_group_fraction,
                    "candidate_rows": result.candidate_rows,
                    "total_rows": result.total_rows,
                    "candidate_row_fraction": result.candidate_row_fraction,
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
            "candidate_row_groups",
            "total_row_groups",
            "candidate_row_group_fraction",
            "candidate_rows",
            "total_rows",
            "candidate_row_fraction",
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
        "median_seconds,stdev_seconds,candidate_row_groups,"
        "total_row_groups,candidate_row_group_fraction,bbox"
    )
    for result in results:
        print(
            f"{result.algorithm},{result.source},{result.query},{result.rows},{result.repeats},"
            f"{result.warmups},{result.best_seconds:.6f},{result.mean_seconds:.6f},"
            f"{result.median_seconds:.6f},{result.stdev_seconds:.6f},"
            f"{result.candidate_row_groups},"
            f"{result.total_row_groups},{result.candidate_row_group_fraction:.6f},"
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
        description="Compare Hilbert, Morton, STR, and Boost packed GeoParquet sorting."
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

    needs_spatial = args.command in ("build", "all") and any(
        algorithm in ("hilbert", "morton") for algorithm in args.algorithms
    )
    con = connect(args.threads, args.memory_limit, load_spatial=needs_spatial)
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
            if args.query_warmups != 0:
                raise SystemExit(
                    "remote benchmarks do not allow warmups; pass --query-warmups 0"
                )
            disable_duckdb_caches(con)
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
            has_remote_output,
            args.threads,
            args.memory_limit,
        )
        print_query_results(query_results)
        write_query_csv(args.metrics_dir / "queries.csv", query_results)
        write_query_runs_csv(args.metrics_dir / "query_runs.csv", query_results)


if __name__ == "__main__":
    main()
