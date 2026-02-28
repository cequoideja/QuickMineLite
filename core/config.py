"""
Application configuration for QuickMineLite
"""


class Config:
    APP_NAME = "QuickMine Lite"
    APP_VERSION = "2.0.0"

    # CSV Import
    DEFAULT_CSV_ENCODING = 'utf-8'
    FALLBACK_CSV_ENCODING = 'latin-1'
    PREVIEW_ROWS = 1000

    # Row limits
    DEFAULT_IMPORT_LIMIT = 10000
    IMPORT_LIMIT_MIN = 100
    IMPORT_LIMIT_MAX = 10_000_000

    # Event Log Viewer
    EVENT_VIEWER_DEFAULT_ROWS = 100
    EVENT_VIEWER_MAX_ROWS = 100_000

    # Analysis
    ANALYSIS_LIMIT_OPTIONS = ["10", "20", "50", "100", "All"]
    ANALYSIS_DEFAULT_LIMIT = "20"

    # DFG defaults
    DFG_DEFAULT_FREQUENCY_THRESHOLD = 0.0
    DFG_DEFAULT_PERFORMANCE_THRESHOLD = 0.0

    # Chart export
    CHART_EXPORT_WIDTH = 1400
    CHART_EXPORT_HEIGHT = 800

    # Preview table
    TABLE_PREVIEW_ROWS = 10
