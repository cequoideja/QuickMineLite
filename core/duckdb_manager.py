"""
DuckDB integration layer for fast analytical queries on event logs.
Replaces SQLite + heavy pandas aggregations with SQL-based analytics.
"""
import duckdb
import pandas as pd
from typing import Optional, Dict, Any


class DuckDBManager:
    """Manages DuckDB in-memory database for fast analytical queries"""

    def __init__(self):
        self.conn = duckdb.connect()
        self._table_registered = False

    def load_dataframe(self, df: pd.DataFrame, table_name: str = 'events'):
        """Register a pandas DataFrame as a DuckDB virtual table (zero-copy)"""
        try:
            self.conn.unregister(table_name)
        except Exception:
            pass
        self.conn.register(table_name, df)
        self._table_registered = True

    def _query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return result as DataFrame"""
        return self.conn.execute(sql).fetchdf()

    def _scalar(self, sql: str):
        """Execute SQL query and return single scalar value"""
        result = self.conn.execute(sql).fetchone()
        return result[0] if result else None

    # -- Summary Statistics ------------------------------------------------

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get global summary statistics via SQL"""
        row = self.conn.execute("""
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT "case:concept:name") as total_cases,
                COUNT(DISTINCT "concept:name") as total_activities,
                MIN("time:timestamp") as start_date,
                MAX("time:timestamp") as end_date
            FROM events
        """).fetchone()
        return {
            'total_events': row[0],
            'total_cases': row[1],
            'total_activities': row[2],
            'start_date': pd.Timestamp(row[3]),
            'end_date': pd.Timestamp(row[4]),
        }

    # -- Activity Distribution --------------------------------------------

    def get_activity_distribution(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Activity frequency distribution"""
        sql = """
            SELECT
                "concept:name" as activity,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM events
            GROUP BY "concept:name"
            ORDER BY count DESC
        """
        if limit:
            sql += f" LIMIT {limit}"
        return self._query(sql)

    # -- Events Over Time -------------------------------------------------

    def get_events_over_time(self, freq: str = 'day') -> pd.DataFrame:
        """Events and cases over time (day/week/month)"""
        trunc = freq if freq in ('day', 'week', 'month') else 'day'
        return self._query(f"""
            SELECT
                DATE_TRUNC('{trunc}', "time:timestamp") as date,
                COUNT(*) as event_count,
                COUNT(DISTINCT "case:concept:name") as case_count
            FROM events
            GROUP BY DATE_TRUNC('{trunc}', "time:timestamp")
            ORDER BY date
        """)

    # -- Case Durations ---------------------------------------------------

    def get_case_durations(self) -> pd.DataFrame:
        """Duration for each case in seconds"""
        return self._query("""
            SELECT
                "case:concept:name" as case_id,
                EXTRACT(EPOCH FROM (MAX("time:timestamp") - MIN("time:timestamp"))) as duration_seconds,
                COUNT(*) as num_events
            FROM events
            GROUP BY "case:concept:name"
        """)

    def get_case_duration_stats(self) -> Dict[str, float]:
        """Aggregated case duration statistics"""
        row = self.conn.execute("""
            WITH case_dur AS (
                SELECT EXTRACT(EPOCH FROM (MAX("time:timestamp") - MIN("time:timestamp"))) as dur
                FROM events
                GROUP BY "case:concept:name"
            )
            SELECT
                AVG(dur), MEDIAN(dur), MIN(dur), MAX(dur), STDDEV(dur)
            FROM case_dur
        """).fetchone()
        return {
            'avg': row[0], 'median': row[1], 'min': row[2],
            'max': row[3], 'std': row[4]
        }

    # -- Events Per Case --------------------------------------------------

    def get_events_per_case_distribution(self) -> pd.DataFrame:
        """Distribution of number of events per case"""
        return self._query("""
            WITH epc AS (
                SELECT COUNT(*) as num_events
                FROM events
                GROUP BY "case:concept:name"
            )
            SELECT num_events, COUNT(*) as case_count
            FROM epc
            GROUP BY num_events
            ORDER BY num_events
        """)

    def get_events_per_case_stats(self) -> Dict[str, float]:
        """Aggregated events-per-case statistics"""
        row = self.conn.execute("""
            WITH epc AS (
                SELECT COUNT(*) as cnt FROM events GROUP BY "case:concept:name"
            )
            SELECT AVG(cnt), MEDIAN(cnt), MIN(cnt), MAX(cnt) FROM epc
        """).fetchone()
        return {'avg': row[0], 'median': row[1], 'min': row[2], 'max': row[3]}

    # -- Start / End Activities -------------------------------------------

    def get_start_activities(self) -> pd.DataFrame:
        """Start activity distribution"""
        return self._query("""
            WITH first_events AS (
                SELECT "concept:name" as activity,
                       ROW_NUMBER() OVER (
                           PARTITION BY "case:concept:name"
                           ORDER BY "time:timestamp"
                       ) as rn
                FROM events
            )
            SELECT activity, COUNT(*) as count,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM first_events WHERE rn = 1
            GROUP BY activity ORDER BY count DESC
        """)

    def get_end_activities(self) -> pd.DataFrame:
        """End activity distribution"""
        return self._query("""
            WITH last_events AS (
                SELECT "concept:name" as activity,
                       ROW_NUMBER() OVER (
                           PARTITION BY "case:concept:name"
                           ORDER BY "time:timestamp" DESC
                       ) as rn
                FROM events
            )
            SELECT activity, COUNT(*) as count,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM last_events WHERE rn = 1
            GROUP BY activity ORDER BY count DESC
        """)

    # -- DFG Edges --------------------------------------------------------

    def get_dfg_edges(self) -> pd.DataFrame:
        """Directly-Follows Graph edges with frequencies (via LEAD)"""
        return self._query("""
            WITH transitions AS (
                SELECT
                    "concept:name" as source,
                    LEAD("concept:name") OVER (
                        PARTITION BY "case:concept:name"
                        ORDER BY "time:timestamp"
                    ) as target
                FROM events
            )
            SELECT source, target, COUNT(*) as frequency
            FROM transitions
            WHERE target IS NOT NULL
            GROUP BY source, target
            ORDER BY frequency DESC
        """)

    def get_performance_edges(self) -> pd.DataFrame:
        """DFG edges with average duration in seconds"""
        return self._query("""
            WITH transitions AS (
                SELECT
                    "concept:name" as source,
                    LEAD("concept:name") OVER (
                        PARTITION BY "case:concept:name"
                        ORDER BY "time:timestamp"
                    ) as target,
                    EXTRACT(EPOCH FROM (
                        LEAD("time:timestamp") OVER (
                            PARTITION BY "case:concept:name"
                            ORDER BY "time:timestamp"
                        ) - "time:timestamp"
                    )) as duration_seconds
                FROM events
            )
            SELECT source, target,
                   COUNT(*) as frequency,
                   AVG(duration_seconds) as avg_duration,
                   MEDIAN(duration_seconds) as median_duration
            FROM transitions
            WHERE target IS NOT NULL
            GROUP BY source, target
            ORDER BY avg_duration DESC
        """)

    # -- Variant Statistics -----------------------------------------------

    def get_variant_statistics(self) -> pd.DataFrame:
        """Process variant statistics"""
        return self._query("""
            WITH case_variants AS (
                SELECT
                    "case:concept:name" as case_id,
                    STRING_AGG("concept:name", ' -> ' ORDER BY "time:timestamp") as variant
                FROM events
                GROUP BY "case:concept:name"
            )
            SELECT
                variant,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM case_variants
            GROUP BY variant
            ORDER BY count DESC
        """)

    # -- Resource Workload ------------------------------------------------

    def get_resource_workload(self) -> Optional[pd.DataFrame]:
        """Resource workload statistics"""
        cols = self._query("SELECT * FROM events LIMIT 0").columns.tolist()
        if 'org:resource' not in cols:
            return None
        return self._query("""
            SELECT
                "org:resource" as resource,
                COUNT(*) as total_events,
                COUNT(DISTINCT "case:concept:name") as unique_cases,
                COUNT(DISTINCT "concept:name") as unique_activities,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as workload_pct
            FROM events
            WHERE "org:resource" IS NOT NULL
            GROUP BY "org:resource"
            ORDER BY total_events DESC
        """)

    # -- Activity Duration Bottlenecks ------------------------------------

    def get_activity_durations(self) -> pd.DataFrame:
        """Activity duration statistics via LEAD"""
        return self._query("""
            WITH act_dur AS (
                SELECT
                    "concept:name" as activity,
                    EXTRACT(EPOCH FROM (
                        LEAD("time:timestamp") OVER (
                            PARTITION BY "case:concept:name"
                            ORDER BY "time:timestamp"
                        ) - "time:timestamp"
                    )) as duration
                FROM events
            )
            SELECT
                activity,
                COUNT(*) as count,
                AVG(duration) as avg_duration,
                MEDIAN(duration) as median_duration,
                STDDEV(duration) as std_duration,
                MIN(duration) as min_duration,
                MAX(duration) as max_duration
            FROM act_dur
            WHERE duration IS NOT NULL
            GROUP BY activity
            ORDER BY avg_duration DESC
        """)

    # -- Waiting Time Bottlenecks -----------------------------------------

    def get_waiting_times(self) -> pd.DataFrame:
        """Transition waiting time statistics"""
        return self._query("""
            WITH transitions AS (
                SELECT
                    LAG("concept:name") OVER (
                        PARTITION BY "case:concept:name"
                        ORDER BY "time:timestamp"
                    ) as from_activity,
                    "concept:name" as to_activity,
                    EXTRACT(EPOCH FROM (
                        "time:timestamp" - LAG("time:timestamp") OVER (
                            PARTITION BY "case:concept:name"
                            ORDER BY "time:timestamp"
                        )
                    )) as waiting_time
                FROM events
            )
            SELECT
                from_activity || ' -> ' || to_activity as transition,
                COUNT(*) as count,
                AVG(waiting_time) as avg_waiting,
                MEDIAN(waiting_time) as median_waiting,
                MIN(waiting_time) as min_waiting,
                MAX(waiting_time) as max_waiting
            FROM transitions
            WHERE from_activity IS NOT NULL
            GROUP BY from_activity, to_activity
            ORDER BY avg_waiting DESC
        """)

    # -- Case Events (for case explorer) ----------------------------------

    def get_case_events(self, case_id: str) -> pd.DataFrame:
        """Get all events for a specific case"""
        return self.conn.execute("""
            SELECT * FROM events
            WHERE "case:concept:name" = ?
            ORDER BY "time:timestamp"
        """, [case_id]).fetchdf()

    # -- Case List --------------------------------------------------------

    def get_case_list(self) -> pd.DataFrame:
        """Get case list with summary info"""
        return self._query("""
            SELECT
                "case:concept:name" as case_id,
                COUNT(*) as num_events,
                MIN("time:timestamp") as start_time,
                MAX("time:timestamp") as end_time,
                EXTRACT(EPOCH FROM (MAX("time:timestamp") - MIN("time:timestamp"))) as duration_seconds
            FROM events
            GROUP BY "case:concept:name"
            ORDER BY start_time
        """)

    # -- Utility ----------------------------------------------------------

    def get_columns(self):
        """Get column names of events table"""
        return self._query("SELECT * FROM events LIMIT 0").columns.tolist()

    def get_paginated_events(self, offset: int = 0, limit: int = 100,
                             columns: list = None) -> pd.DataFrame:
        """Get paginated events"""
        if columns:
            cols_str = ', '.join(f'"{c}"' for c in columns)
        else:
            cols_str = '*'
        return self._query(f"""
            SELECT {cols_str} FROM events
            ORDER BY "time:timestamp"
            LIMIT {limit} OFFSET {offset}
        """)

    def get_total_event_count(self) -> int:
        """Get total number of events"""
        return self._scalar("SELECT COUNT(*) FROM events")

    def close(self):
        """Close DuckDB connection"""
        if self.conn:
            self.conn.close()
