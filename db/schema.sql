CREATE TABLE IF NOT EXISTS index_history (
    index_key TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    close REAL,
    change_pct REAL,
    pe_ttm REAL,
    source TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (index_key, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_index_history_date
    ON index_history (trade_date);

CREATE TABLE IF NOT EXISTS index_latest (
    index_key TEXT PRIMARY KEY,
    index_name TEXT NOT NULL,
    csi_symbol TEXT NOT NULL,
    history_date TEXT,
    history_close REAL,
    history_change_pct REAL,
    price REAL,
    change_pct REAL,
    quote_source TEXT,
    pe_ttm REAL,
    earnings_yield_pct REAL,
    pe_percentile_5y REAL,
    pe_window_5y_samples INTEGER,
    pe_window_5y_start TEXT,
    pe_percentile_10y REAL,
    pe_window_10y_samples INTEGER,
    pe_window_10y_start TEXT,
    pe_latest_date TEXT,
    source TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bond_yield_daily (
    trade_date TEXT PRIMARY KEY,
    yield_10y REAL NOT NULL,
    source TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS app_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS index_strategy_settings (
    index_key TEXT PRIMARY KEY,
    initial_amount REAL NOT NULL,
    initial_index_price REAL NOT NULL,
    strategy_start_date TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
