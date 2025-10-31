# Schwab WebSocket Client

A Python client for streaming real-time market data from Charles Schwab's WebSocket API. The client subscribes to both equity chart data and options level-one data, saving all information to CSV files.

## Features

- **WebSocket Connection**: Establishes secure WebSocket connection with Schwab Streaming API
- **Chart Data Streaming**: Subscribes to `CHART_EQUITY` data for equity symbols
- **Options Data Streaming**: Subscribes to `LEVELONE_OPTIONS` data for option contracts
- **Market Hours Handling**: Automatically waits for market open (9:30 AM ET) and disconnects at market close (4:00:30 PM ET)
- **CSV Data Storage**: Saves all streamed data to CSV files organized by symbol
- **Timezone Aware**: All time operations use ET timezone regardless of local timezone

## Requirements

- Python 3.8+
- Virtual environment (venv)
- Valid Schwab access token

## Installation

1. Create and activate virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuration

1. **Access Token**: Place your Schwab access token in `schwab_access_token.txt`

   - The file should contain only the access token (no quotes, no extra characters)

2. **Equity Symbols**: Edit `symbols.txt` with comma-separated equity symbols

   - Example: `SPY,TSLA,AAPL`

3. **Option Symbols**: Edit `options_symbols.txt` with comma-separated option contract symbols
   - Example: `AAPL  251031C00110000,AAPL  251031C00120000`
   - Note: Use exact option contract symbols as provided by Schwab

## Usage

### Basic Usage

Run the client with default settings:

```bash
source .venv/bin/activate  # If not already activated
python schwab_websocket_client.py
```

### Command-Line Options

All file paths and output directory are configurable via command-line arguments:

```bash
python schwab_websocket_client.py [OPTIONS]
```

**Available Options:**

- `--symbols`, `-s`: Path to equity symbols file (default: `symbols.txt`)
- `--options`, `-o`: Path to option symbols file (default: `options_symbols.txt`)
- `--token`, `-t`: Path to access token file (default: `schwab_access_token.txt`)
- `--data-dir`, `-d`: Output directory for CSV files (default: `data`)
- `--debug`: Enable debug mode for verbose logging

**Examples:**

```bash
# Use custom symbol files
python schwab_websocket_client.py --symbols my_stocks.txt --options my_options.txt

# Use custom data output directory
python schwab_websocket_client.py --data-dir /path/to/output

# Enable debug mode with custom paths
python schwab_websocket_client.py --debug --symbols stocks.txt --data-dir market_data

# Use all options together
python schwab_websocket_client.py --symbols equities.txt --options contracts.txt --token my_token.txt --data-dir ./output --debug

# Test with copy files (if you have symbols_copy.txt, options_symbols_copy.txt, etc.)
python schwab_websocket_client.py --symbols symbols_copy.txt --options options_symbols_copy.txt --token schwab_access_token_copy.txt --data-dir test_output
```

### What Happens

The client will:

1. Load symbols from `symbols.txt` and `options_symbols.txt`
2. Wait until market open (9:30 AM ET) if started before market hours
3. Connect to Schwab WebSocket API
4. Subscribe to chart data for equity symbols
5. Subscribe to options data for option symbols
6. Save all incoming data to CSV files (configurable via `--data-dir`):
   - Equity data: `{data-dir}/equity/SYMBOL.csv`
   - Options data: `{data-dir}/options/SYMBOL.csv`
7. Automatically disconnect at market close (4:00:30 PM ET)

## Data Format

Data is saved to CSV files (default location: `data/` folder, configurable via `--data-dir`):

- Equity data: `{data-dir}/equity/SYMBOL.csv`
- Options data: `{data-dir}/options/SYMBOL.csv`

**See the `data/` folder for example output files showing the exact format and column structure.**

### Chart Data (CHART_EQUITY)

CSV files contain all fields from the API plus an ET time column:

- `sequence`: Sequence number
- `key`: Symbol
- `open`, `high`, `low`, `close`: OHLC prices
- `volume`: Volume
- `time`: Timestamp in milliseconds since epoch (original API field)
- `chart_day`: Chart day
- `time_et`: Human-readable ET datetime (added column)

Example file: `data/equity/SPY.csv`

### Options Data (LEVELONE_OPTIONS)

CSV files contain all 56 fields from the API plus an ET time column:

- All standard option fields (bid, ask, last_price, greeks, etc.)
- `quote_time`, `trade_time`, `indicative_quote_time`: Timestamps in milliseconds
- `time_et`: Human-readable ET datetime (added column, uses best available timestamp)

Example file: `data/options/AAPL251031C00110000.csv`

## Dependencies

- `httpx`: HTTP client for API requests
- `websocket-client`: WebSocket client library
- `pytz`: Timezone handling
- `pandas`: Data manipulation and CSV operations

## Notes

- Market hours are enforced (9:30 AM - 4:00:30 PM ET)
- All timestamps are converted to ET timezone
- Data is appended to CSV files as it streams
- The client handles partial updates for options data (merges with previous values)
- Weekend detection prevents running on non-trading days
