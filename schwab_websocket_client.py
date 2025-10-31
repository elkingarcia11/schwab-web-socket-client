"""
Schwab WebSocket Client
Establishes connection with Schwab via WebSocket and subscribes to:
1. Chart data based on symbols.txt
2. Options data based on symbols.txt
"""
from datetime import datetime
from typing import List, Optional, Dict

import json
import time
import threading
import os
import argparse

import httpx
import websocket
import pytz
import pandas as pd


class SchwabWebSocketClient:
    """Schwab WebSocket client for streaming chart and options data"""

    def __init__(self, debug: bool = False, symbols_filepath: str = 'symbols.txt',
                 option_symbols_filepath: str = 'options_symbols.txt',
                 access_token_filepath: str = 'schwab_access_token.txt',
                 data_output_dir: str = 'data'):
        self.debug = debug
        self.access_token_filepath = access_token_filepath
        self.data_output_dir = data_output_dir

        # Read equity symbols from file
        self.symbols = self.load_symbols_from_file(symbols_filepath)

        # Read option symbols from file
        self.option_symbols = self.load_symbols_from_file(
            option_symbols_filepath)

        # WebSocket connection
        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        self.connected = False
        self.request_id = 1
        self.subscriptions = {}

        # Store previous option values for merging partial updates
        self.previous_option_values: Dict[str, Dict] = {}

        # Market hours (ET timezone) - always use ET regardless of local timezone
        self.et_tz = pytz.timezone('US/Eastern')
        # Store as time objects (used for comparison with ET time)
        self.market_open_time = datetime.strptime(
            '09:30:00', '%H:%M:%S').time()
        self.market_close_time = datetime.strptime(
            '16:00:30', '%H:%M:%S').time()

        # Create data directories if they don't exist
        os.makedirs(f'{self.data_output_dir}/equity', exist_ok=True)
        os.makedirs(f'{self.data_output_dir}/options', exist_ok=True)

        # CHART_EQUITY field mappings based on Schwab API
        # Fields 2-6 are OHLCV (Open, High, Low, Close, Volume) and field 7 is timestamp
        self.chart_equity_fields = {
            0: 'key',        # Symbol (ticker symbol)
            1: 'sequence',   # Sequence - Identifies the candle minute
            2: 'open',       # Open Price - Opening price for the minute
            3: 'high',       # High Price - Highest price for the minute
            4: 'low',        # Low Price - Chart's lowest price for the minute
            5: 'close',      # Close Price - Closing price for the minute
            6: 'volume',     # Volume - Total volume for the minute
            7: 'time',       # Chart Time - Milliseconds since Epoch
            8: 'chart_day'   # Chart Day
        }

        # Get user preferences and store streamer info
        self.user_preferences = self.get_user_preferences()
        if not self.user_preferences or 'streamerInfo' not in self.user_preferences:
            raise Exception("Could not get user preferences")

        # Store streamer info
        self.streamer_info = self.user_preferences['streamerInfo'][0]
        self.schwab_websocket_client_customer_id = self.streamer_info.get(
            'schwabClientCustomerId')
        if not self.schwab_websocket_client_customer_id:
            raise Exception("No SchwabClientCustomerId in streamer info")

    def load_symbols_from_file(self, filepath: str) -> List[str]:
        """Load symbols from a comma-separated file"""
        try:
            if not os.path.exists(filepath):
                print(
                    f"⚠️ Symbols file {filepath} not found. Using empty list.")
                return []

            with open(filepath, 'r') as f:
                content = f.read().strip()
                if not content:
                    return []

                # Split by comma and clean up
                symbols = [s.strip() for s in content.split(',') if s.strip()]
                print(
                    f"✅ Loaded {len(symbols)} symbols from {filepath}: {', '.join(symbols)}")
                return symbols
        except Exception as e:
            print(f"❌ Error loading symbols from {filepath}: {e}")
            return []

    def get_access_token(self) -> str:
        """Read access token from file"""
        try:
            if not os.path.exists(self.access_token_filepath):
                raise Exception(
                    f"Access token file not found: {self.access_token_filepath}")

            with open(self.access_token_filepath, 'r') as f:
                token = f.read().strip()

            if not token:
                raise Exception(
                    f"Access token file is empty: {self.access_token_filepath}")

            # Return token as-is (no character removal)
            return token

        except Exception as e:
            print(
                f"❌ Error reading access token from {self.access_token_filepath}: {e}")
            raise

    def get_user_preferences(self):
        """Get user preferences using the access token from file"""
        try:
            # Read token from file
            access_token = self.get_access_token()
            if not access_token:
                raise Exception("Failed to get valid access token")

            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            }

            url = "https://api.schwabapi.com/trader/v1/userPreference"

            with httpx.Client() as client:
                response = client.get(url, headers=headers)

            if response.status_code != 200:
                raise Exception(
                    f"User preferences request failed: {response.status_code} - {response.text}")

            return response.json()

        except Exception as e:
            if self.debug:
                print(f"❌ Error getting user preferences: {e}")
            raise

    def wait_for_market_open(self):
        """
        Check if it's before 9:30:00 AM ET and wait until market opens if needed.
        All time comparisons use ET timezone, regardless of local timezone.
        Returns True if market is open or will be open today, False if it's weekend.
        """
        # Always get current time in ET timezone
        now_et = datetime.now(self.et_tz)
        current_time_et = now_et.time()
        current_date_et = now_et.date()

        # Check if it's weekend (based on ET date)
        if current_date_et.weekday() >= 5:  # Saturday = 5, Sunday = 6
            print(
                f"📅 Weekend detected ({current_date_et.strftime('%A')}) in ET timezone, market is closed")
            return False

        # Check if it's before market hours (all comparisons in ET)
        if current_time_et < self.market_open_time:
            # Calculate time until market open in ET timezone
            # Combine date and time, then localize to ET (handles DST automatically)
            market_open_dt = self.et_tz.localize(
                datetime.combine(current_date_et, self.market_open_time)
            )

            wait_seconds = (market_open_dt - now_et).total_seconds()

            if wait_seconds > 0:
                wait_hours = int(wait_seconds // 3600)
                wait_minutes = int((wait_seconds % 3600) // 60)
                wait_secs = int(wait_seconds % 60)

                print(
                    f"⏰ Before market hours (ET time). Current ET time: {now_et.strftime('%H:%M:%S %Z')}")
                print(
                    f"📈 Market opens at: {market_open_dt.strftime('%H:%M:%S %Z')}")
                print(
                    f"⏳ Waiting {wait_hours:02d}:{wait_minutes:02d}:{wait_secs:02d} until market open...")

                # Wait with progress indicator (recalculate ET time periodically)
                start_time = time.time()
                while time.time() - start_time < wait_seconds:
                    # Recalculate remaining time based on current ET time
                    remaining_now_et = datetime.now(self.et_tz)
                    remaining = (market_open_dt -
                                 remaining_now_et).total_seconds()
                    if remaining <= 0:
                        break

                    remaining_hours = int(remaining // 3600)
                    remaining_minutes = int((remaining % 3600) // 60)
                    remaining_secs = int(remaining % 60)

                    print(
                        f"\r⏳ Waiting: {remaining_hours:02d}:{remaining_minutes:02d}:{remaining_secs:02d} remaining (ET time)...", end='', flush=True)
                    time.sleep(1)

                print(f"\n🎉 Market is now open (ET time)! Starting streaming...")
            else:
                print(
                    f"✅ Market is already open (current ET time: {now_et.strftime('%H:%M:%S %Z')})")
        else:
            print(
                f"✅ Market is already open (current ET time: {now_et.strftime('%H:%M:%S %Z')})")

        return True

    def is_after_market_close(self):
        """
        Check if current time is after 4:00:30 PM ET.
        All time comparisons use ET timezone, regardless of local timezone.
        """
        # Always get current time in ET timezone
        now_et = datetime.now(self.et_tz)
        current_date_et = now_et.date()

        # Create market close datetime for today in ET timezone
        # Localizing handles DST automatically
        market_close_dt = self.et_tz.localize(
            datetime.combine(current_date_et, self.market_close_time)
        )

        # Compare timezone-aware datetimes (both in ET)
        return now_et >= market_close_dt

    def parse_chart_data(self, content_item: dict) -> Dict:
        """Parse CHART_EQUITY message content using field mappings"""
        try:
            parsed = {}

            for field_key, field_value in content_item.items():
                if field_key.isdigit():
                    field_index = int(field_key)
                    if field_index in self.chart_equity_fields:
                        field_name = self.chart_equity_fields[field_index]

                        # Convert to appropriate data type based on field
                        if field_name in ['open', 'high', 'low', 'close', 'volume']:
                            parsed[field_name] = float(field_value)
                        elif field_name in ['time', 'chart_day', 'sequence']:
                            parsed[field_name] = int(field_value)
                        else:
                            parsed[field_name] = field_value
                elif field_key in ['key', 'seq']:
                    if field_key == 'seq':
                        parsed['sequence'] = int(field_value)
                    else:
                        parsed[field_key] = field_value

            return parsed

        except Exception as e:
            if self.debug:
                print(f"❌ Error parsing CHART_EQUITY data: {e}")
            return {}

    def parse_option_data(self, content_item: dict) -> Dict:
        """Parse LEVELONE_OPTIONS message content, merging with previous values"""
        try:
            symbol = content_item.get('key', '')
            if not symbol:
                if self.debug:
                    print(
                        f"⚠️ Warning: Empty symbol in option data: {content_item}")
                return {}

            # Get previous values for this symbol, or create default structure
            previous_values = self.previous_option_values.get(symbol, {
                'symbol': '',              # 0: Symbol
                'description': '',         # 1: Description
                'bid_price': 0,           # 2: Bid Price
                'ask_price': 0,           # 3: Ask Price
                'last_price': 0,          # 4: Last Price
                'high_price': 0,          # 5: High Price
                'low_price': 0,           # 6: Low Price
                'close_price': 0,         # 7: Close Price
                'total_volume': 0,         # 8: Total Volume
                'open_interest': 0,       # 9: Open Interest
                'volatility': 0,          # 10: Volatility
                'money_intrinsic_value': 0,  # 11: Money Intrinsic Value
                'expiration_year': 0,     # 12: Expiration Year
                'multiplier': 0,          # 13: Multiplier
                'digits': 0,              # 14: Digits
                'open_price': 0,          # 15: Open Price
                'bid_size': 0,            # 16: Bid Size
                'ask_size': 0,            # 17: Ask Size
                'last_size': 0,           # 18: Last Size
                'net_change': 0,          # 19: Net Change
                'strike_price': 0,        # 20: Strike Price
                'contract_type': '',      # 21: Contract Type
                'underlying': '',         # 22: Underlying
                'expiration_month': 0,    # 23: Expiration Month
                'deliverables': '',       # 24: Deliverables
                'time_value': 0,          # 25: Time Value
                'expiration_day': 0,      # 26: Expiration Day
                'days_to_expiration': 0,  # 27: Days to Expiration
                'delta': 0,               # 28: Delta
                'gamma': 0,               # 29: Gamma
                'theta': 0,               # 30: Theta
                'vega': 0,                # 31: Vega
                'rho': 0,                 # 32: Rho
                'security_status': '',    # 33: Security Status
                'theoretical_option_value': 0,  # 34: Theoretical Option Value
                'underlying_price': 0,    # 35: Underlying Price
                'uv_expiration_type': '',  # 36: UV Expiration Type
                'mark_price': 0,          # 37: Mark Price
                # 38: Quote Time (milliseconds since Epoch)
                'quote_time': 0,
                # 39: Trade Time (milliseconds since Epoch)
                'trade_time': 0,
                'exchange': '',          # 40: Exchange
                'exchange_name': '',      # 41: Exchange Name
                'last_trading_day': 0,    # 42: Last Trading Day
                'settlement_type': '',    # 43: Settlement Type
                'net_percent_change': 0,  # 44: Net Percent Change
                'mark_price_net_change': 0,  # 45: Mark Price Net Change
                'mark_price_percent_change': 0,  # 46: Mark Price Percent Change
                'implied_yield': 0,       # 47: Implied Yield
                'is_penny_pilot': False,  # 48: isPennyPilot
                'option_root': '',        # 49: Option Root
                'week_52_high': 0,        # 50: 52 Week High
                'week_52_low': 0,         # 51: 52 Week Low
                'indicative_ask_price': 0,  # 52: Indicative Ask Price
                'indicative_bid_price': 0,  # 53: Indicative Bid Price
                'indicative_quote_time': 0,  # 54: Indicative Quote Time
                'exercise_type': ''       # 55: Exercise Type
            })

            # Create new option data by merging previous values with new updates
            option_data = previous_values.copy()

            # Field mapping for LEVELONE_OPTIONS
            field_mapping = {
                '0': 'symbol',
                '1': 'description',
                '2': 'bid_price',
                '3': 'ask_price',
                '4': 'last_price',
                '5': 'high_price',
                '6': 'low_price',
                '7': 'close_price',
                '8': 'total_volume',
                '9': 'open_interest',
                '10': 'volatility',
                '11': 'money_intrinsic_value',
                '12': 'expiration_year',
                '13': 'multiplier',
                '14': 'digits',
                '15': 'open_price',
                '16': 'bid_size',
                '17': 'ask_size',
                '18': 'last_size',
                '19': 'net_change',
                '20': 'strike_price',
                '21': 'contract_type',
                '22': 'underlying',
                '23': 'expiration_month',
                '24': 'deliverables',
                '25': 'time_value',
                '26': 'expiration_day',
                '27': 'days_to_expiration',
                '28': 'delta',
                '29': 'gamma',
                '30': 'theta',
                '31': 'vega',
                '32': 'rho',
                '33': 'security_status',
                '34': 'theoretical_option_value',
                '35': 'underlying_price',
                '36': 'uv_expiration_type',
                '37': 'mark_price',
                '38': 'quote_time',
                '39': 'trade_time',
                '40': 'exchange',
                '41': 'exchange_name',
                '42': 'last_trading_day',
                '43': 'settlement_type',
                '44': 'net_percent_change',
                '45': 'mark_price_net_change',
                '46': 'mark_price_percent_change',
                '47': 'implied_yield',
                '48': 'is_penny_pilot',
                '49': 'option_root',
                '50': 'week_52_high',
                '51': 'week_52_low',
                '52': 'indicative_ask_price',
                '53': 'indicative_bid_price',
                '54': 'indicative_quote_time',
                '55': 'exercise_type'
            }

            # Update fields that are present in the new message - capture ALL fields
            for field_key, field_name in field_mapping.items():
                if field_key in content_item:
                    value = content_item[field_key]
                    # Convert to appropriate type
                    if field_name in ['bid_price', 'ask_price', 'last_price', 'high_price', 'low_price',
                                      'close_price', 'volatility', 'money_intrinsic_value', 'strike_price',
                                      'time_value', 'delta', 'gamma', 'theta', 'vega', 'rho',
                                      'theoretical_option_value', 'underlying_price', 'mark_price',
                                      'net_percent_change', 'mark_price_net_change', 'mark_price_percent_change',
                                      'implied_yield', 'week_52_high', 'week_52_low', 'indicative_ask_price',
                                      'indicative_bid_price']:
                        try:
                            option_data[field_name] = float(
                                value) if value else 0
                        except (ValueError, TypeError):
                            option_data[field_name] = 0
                    elif field_name in ['total_volume', 'open_interest', 'expiration_year', 'multiplier',
                                        'digits', 'open_price', 'bid_size', 'ask_size', 'last_size',
                                        'expiration_month', 'expiration_day', 'days_to_expiration',
                                        'last_trading_day', 'quote_time', 'trade_time', 'indicative_quote_time']:
                        try:
                            option_data[field_name] = int(
                                value) if value else 0
                        except (ValueError, TypeError):
                            option_data[field_name] = 0
                    elif field_name == 'is_penny_pilot':
                        option_data[field_name] = bool(
                            value) if value else False
                    else:
                        option_data[field_name] = str(value) if value else ''

            # Capture ANY additional fields from the API that aren't in our mapping (preserve all data)
            for field_key, field_value in content_item.items():
                if field_key not in field_mapping and field_key != 'key':
                    # Keep any unmapped fields as-is
                    option_data[f'field_{field_key}'] = field_value

            # Store the updated values for next time
            self.previous_option_values[symbol] = option_data.copy()

            return option_data

        except Exception as e:
            if self.debug:
                print(f"❌ Error parsing option data: {e}")
            return {}

    def save_chart_data_to_csv(self, symbol: str, data: dict):
        """
        Save a new row of chart data to CSV file.

        Args:
            symbol (str): Equity symbol name
            data (dict): Chart data row to save (should contain timestamp, open, high, low, close, volume)
        """
        try:
            # Create equity directory if it doesn't exist
            os.makedirs(f'{self.data_output_dir}/equity', exist_ok=True)

            # Clean the symbol for filename (remove spaces and special characters)
            clean_symbol = symbol.replace(' ', '').replace(
                '/', '_').replace('\\', '_')

            # Define CSV file path
            csv_file = f'{self.data_output_dir}/equity/{clean_symbol}.csv'

            # Add ET time column (keep original time field as milliseconds)
            if 'time' in data and isinstance(data['time'], int) and data['time'] > 0:
                try:
                    # Convert milliseconds since epoch to ET timezone datetime string
                    timestamp_dt = pd.Timestamp(
                        data['time'], unit='ms', tz='US/Eastern')
                    # Add ET time column (keep original time field unchanged)
                    data['time_et'] = timestamp_dt.strftime(
                        '%Y-%m-%d %H:%M:%S %Z')  # type: ignore
                except (ValueError, OverflowError, AttributeError):
                    pass  # Skip invalid timestamps

            # Convert data to DataFrame
            df_new = pd.DataFrame([data])

            # Check if file exists
            if os.path.exists(csv_file):
                # Append to existing file
                df_new.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                # Create new file with headers
                df_new.to_csv(csv_file, index=False)

            if self.debug:
                print(f"💾 Saved chart data for {symbol} to {csv_file}")

        except Exception as e:
            print(f"❌ Error saving chart data for {symbol}: {e}")

    def save_options_data_to_csv(self, symbol: str, data: dict):
        """
        Save a new row of options data to CSV file.

        Args:
            symbol (str): Option symbol name
            data (dict): Option data row to save
        """
        try:
            # Create options directory if it doesn't exist
            os.makedirs(f'{self.data_output_dir}/options', exist_ok=True)

            # Clean the symbol for filename (remove spaces and special characters)
            clean_symbol = symbol.replace(' ', '').replace(
                '/', '_').replace('\\', '_')

            # Define CSV file path
            csv_file = f'{self.data_output_dir}/options/{clean_symbol}.csv'

            # Add ET time column for options (keep all original timestamp fields unchanged)
            # Use field 54 (Indicative Quote Time) as primary timestamp, fall back to quote_time or trade_time
            timestamp_ms = 0
            timestamp_field_name = None
            if 'indicative_quote_time' in data and isinstance(data['indicative_quote_time'], int) and data['indicative_quote_time'] > 0:
                timestamp_ms = data['indicative_quote_time']
                timestamp_field_name = 'indicative_quote_time'
            elif 'quote_time' in data and isinstance(data['quote_time'], int) and data['quote_time'] > 0:
                timestamp_ms = data['quote_time']
                timestamp_field_name = 'quote_time'
            elif 'trade_time' in data and isinstance(data['trade_time'], int) and data['trade_time'] > 0:
                timestamp_ms = data['trade_time']
                timestamp_field_name = 'trade_time'

            # Convert the timestamp to ET datetime and add as new column (keep original unchanged)
            if timestamp_ms > 0:
                try:
                    timestamp_dt = pd.Timestamp(
                        timestamp_ms, unit='ms', tz='US/Eastern')
                    # Add ET time column (keep all original timestamp fields unchanged)
                    data['time_et'] = timestamp_dt.strftime(
                        '%Y-%m-%d %H:%M:%S %Z')  # type: ignore
                except (ValueError, OverflowError, AttributeError):
                    pass  # Skip invalid timestamps

            # Keep all original fields - don't remove anything

            # Convert data to DataFrame
            df_new = pd.DataFrame([data])

            # Check if file exists
            if os.path.exists(csv_file):
                # Append to existing file
                df_new.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                # Create new file with headers
                df_new.to_csv(csv_file, index=False)

            if self.debug:
                print(f"💾 Saved options data for {symbol} to {csv_file}")

        except Exception as e:
            print(f"❌ Error saving options data for {symbol}: {e}")

    def connect(self):
        """Connect to WebSocket and start streaming"""
        try:
            # Get streamer info from user preferences
            if not self.streamer_info:
                raise Exception("No streamer info available")

            # Get WebSocket URL
            ws_url = self.streamer_info.get('streamerSocketUrl')
            if not ws_url:
                raise Exception("No WebSocket URL in streamer info")

            print(f"🔌 Connecting to WebSocket: {ws_url}")

            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )

            # Start WebSocket connection in a separate thread
            def run_ws():
                if self.ws:
                    self.ws.run_forever()

            # Start the WebSocket thread
            ws_thread = threading.Thread(target=run_ws)
            ws_thread.daemon = True
            ws_thread.start()

            # Wait for connection and login
            timeout = 30
            start_time = time.time()
            while not self.connected and time.time() - start_time < timeout:
                time.sleep(0.1)

            if not self.connected:
                raise Exception(
                    "Failed to connect and login to WebSocket within timeout")

            print("✅ Successfully connected and logged in to WebSocket")

            # Keep the main thread alive and check for market close
            while self.running and self.connected:
                # Check if it's after market close (4:00:30 PM ET)
                if self.is_after_market_close():
                    print(
                        "🕐 Market close time (4:00:30 PM ET) reached, disconnecting...")
                    self.disconnect()
                    break
                time.sleep(1)

        except Exception as e:
            print(f"❌ WebSocket error: {str(e)}")
            if self.ws:
                self.ws.close()
            raise

    def disconnect(self):
        """Disconnect from WebSocket API"""
        self.running = False
        if self.ws:
            self.ws.close()
        self.connected = False
        print("🔌 Disconnected from Schwab Streaming API")

    def on_open(self, _):
        """Handle WebSocket connection open"""
        print("🔗 WebSocket connected, attempting login...")
        self.running = True
        self.login()  # Send login request immediately after connection

    def login(self):
        """Send login request to WebSocket"""
        try:
            # Read token from file
            access_token = self.get_access_token()
            if not access_token:
                raise Exception("Failed to get valid access token")

            if self.streamer_info is not None:
                # Prepare login request
                request = {
                    "service": "ADMIN",
                    "command": "LOGIN",
                    "requestid": str(self.request_id),
                    "SchwabClientCustomerId": self.schwab_websocket_client_customer_id,
                    "SchwabClientCorrelId": self.streamer_info.get("schwabClientCorrelId", ""),
                    "parameters": {
                        "Authorization": access_token,
                        "SchwabClientChannel": self.streamer_info.get("schwabClientChannel", ""),
                        "SchwabClientFunctionId": self.streamer_info.get("schwabClientFunctionId", "")
                    }
                }

                if self.debug:
                    print(f"📤 Sending login: {json.dumps(request, indent=2)}")

                # Send login request
                if self.ws:
                    self.ws.send(json.dumps(request))
                self.request_id += 1

        except Exception as e:
            print(f"❌ Login error: {str(e)}")
            raise

    def subscribe_chart_data(self, symbols: List[str]):
        """Subscribe to CHART_EQUITY data for the given symbols"""
        if not self.connected:
            print("❌ Not connected to WebSocket")
            return

        if not symbols:
            print("⚠️ No symbols provided for chart data subscription")
            return

        try:
            # Prepare the subscription request
            request = {
                "service": "CHART_EQUITY",
                "command": "SUBS",
                "requestid": self.request_id,
                "SchwabClientCustomerId": self.schwab_websocket_client_customer_id,
                "SchwabClientCorrelId": f"chart_{int(time.time() * 1000)}",
                "parameters": {
                    "keys": ",".join(symbols),
                    # All fields: key,sequence,open,high,low,close,volume,time,chart_day
                    "fields": "0,1,2,3,4,5,6,7,8"
                }
            }

            if self.debug:
                print(
                    f"📤 Sending CHART_EQUITY subscription request: {json.dumps(request, indent=2)}")

            # Send the subscription request
            if self.ws:
                self.ws.send(json.dumps(request))
            self.request_id += 1

            # Store the subscription
            self.subscriptions["CHART_EQUITY"] = symbols

            print(
                f"✅ Subscribed to CHART_EQUITY data for: {', '.join(symbols)}")

        except Exception as e:
            print(f"❌ Error subscribing to CHART_EQUITY data: {e}")
            raise

    def subscribe_options_data(self, symbols: List[str]):
        """Subscribe to LEVELONE_OPTIONS data for the given symbols"""
        if not self.connected:
            print("❌ Not connected to WebSocket")
            return

        if not symbols:
            print("⚠️ No symbols provided for options data subscription")
            return

        # Note: For options, you typically need option contract symbols (e.g., SPY_123123C00123456)
        # not equity symbols. This is a basic implementation - you may need to convert
        # equity symbols to option symbols first.
        try:
            # Prepare the subscription request for option data
            request = {
                "service": "LEVELONE_OPTIONS",
                "command": "SUBS",
                "requestid": self.request_id,
                "SchwabClientCustomerId": self.schwab_websocket_client_customer_id,
                "SchwabClientCorrelId": f"option_{int(time.time() * 1000)}",
                "parameters": {
                    "keys": ",".join(symbols),
                    "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55"  # All available fields
                }
            }

            if self.debug:
                print(
                    f"📤 Sending LEVELONE_OPTIONS subscription request: {json.dumps(request, indent=2)}")

            # Send the subscription request
            if self.ws:
                self.ws.send(json.dumps(request))
            self.request_id += 1

            # Store the subscription
            self.subscriptions["LEVELONE_OPTIONS"] = symbols

            print(
                f"✅ Subscribed to LEVELONE_OPTIONS data for: {', '.join(symbols)}")

        except Exception as e:
            print(f"❌ Error subscribing to LEVELONE_OPTIONS data: {e}")
            raise

    def on_message(self, _, message):
        """Handle WebSocket messages"""
        try:
            data = json.loads(message)

            if self.debug:
                print(f"📨 Received message: {json.dumps(data, indent=2)}")

            # Handle different message types
            if isinstance(data, dict):
                if "response" in data:
                    # Login response
                    response_data = data["response"][0]
                    if response_data.get("command") == "LOGIN":
                        content = response_data.get("content", {})
                        code = content.get("code", -1)
                        if code == 0:
                            print("✅ WebSocket login successful")
                            msg = content.get("msg", "")
                            if "status=" in msg:
                                status = msg.split("status=")[1].split(
                                    ";")[0] if ";" in msg else msg.split("status=")[1]
                                print(f"📊 Account status: {status}")
                            self.connected = True

                            # Subscribe to chart and options data after successful login
                            print("📊 Subscribing to chart and options data...")
                            if self.symbols:
                                self.subscribe_chart_data(self.symbols)
                            else:
                                print(
                                    "⚠️ No equity symbols loaded, skipping chart data subscription")

                            if self.option_symbols:
                                self.subscribe_options_data(
                                    self.option_symbols)
                            else:
                                print(
                                    "⚠️ No option symbols loaded, skipping options data subscription")
                        else:
                            print(
                                f"❌ WebSocket login failed with code: {code}")
                            print(
                                f"   Message: {content.get('msg', 'Unknown error')}")
                            self.connected = False

                elif "notify" in data:
                    # Heartbeat or other notifications
                    notify_data = data["notify"][0]
                    if notify_data.get("heartbeat"):
                        if self.debug:
                            print("💓 Heartbeat received")
                    elif notify_data.get("service") == "ADMIN":
                        content = notify_data.get("content", {})
                        if content.get("code") == 30:  # Empty subscription
                            print("⚠️ Empty subscription detected, resubscribing...")
                            if self.symbols:
                                self.subscribe_chart_data(self.symbols)
                            if self.option_symbols:
                                self.subscribe_options_data(
                                    self.option_symbols)

                elif "data" in data:
                    # Market data
                    for data_item in data["data"]:
                        service = data_item.get("service")
                        content = data_item.get("content", [])

                        if service == "CHART_EQUITY":
                            if self.debug:
                                print(
                                    f"📈 Received CHART_EQUITY data: {len(content)} items")
                            # Process and save chart data
                            for candle_data in content:
                                if self.debug:
                                    print(f"   Candle data: {candle_data}")

                                # Parse the chart data
                                parsed_candle = self.parse_chart_data(
                                    candle_data)
                                if parsed_candle and 'key' in parsed_candle:
                                    symbol = parsed_candle.get('key')
                                    # Only save if symbol is in our subscription list
                                    if symbol in self.symbols:
                                        # Save to CSV
                                        self.save_chart_data_to_csv(
                                            symbol, parsed_candle)
                                    elif self.debug:
                                        print(
                                            f"⚠️ Symbol {symbol} not in subscription list, skipping save")

                        elif service == "LEVELONE_OPTIONS":
                            if self.debug:
                                print(
                                    f"📊 Received LEVELONE_OPTIONS data: {len(content)} items")
                            # Process and save options data
                            for option_data in content:
                                if self.debug:
                                    print(f"   Option data: {option_data}")

                                # Parse the option data
                                parsed_option = self.parse_option_data(
                                    option_data)
                                if parsed_option:
                                    # Get symbol from parsed data or raw data
                                    symbol = parsed_option.get(
                                        'symbol', '') or option_data.get('key', '')
                                    if not symbol:
                                        continue

                                    # Normalize symbols for comparison (remove spaces)
                                    normalized_symbol = symbol.replace(' ', '')
                                    normalized_option_symbols = [
                                        s.replace(' ', '') for s in self.option_symbols]

                                    # Only save if symbol matches any in our subscription list
                                    if normalized_symbol in normalized_option_symbols or any(norm_sym in normalized_symbol for norm_sym in normalized_option_symbols):
                                        # Save to CSV (save whenever we have valid data)
                                        if parsed_option.get('last_price', 0) != 0 or parsed_option.get('bid_price', 0) != 0:
                                            self.save_options_data_to_csv(
                                                symbol, parsed_option)
                                    elif self.debug:
                                        print(
                                            f"⚠️ Symbol {symbol} not in option subscription list, skipping save")

                else:
                    # Handle unexpected message types
                    if self.debug:
                        print(
                            f"⚠️ Unexpected message type received: {list(data.keys())}")

        except Exception as e:
            print(f"❌ Message handling error: {str(e)}")
            if self.debug:
                print(f"   Raw message: {message}")
            raise

    def on_error(self, _, error):
        """Handle WebSocket errors"""
        print(f"❌ WebSocket error: {str(error)}")
        self.connected = False

    def on_close(self, _, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        print(
            f"WebSocket connection closed: {close_status_code} - {close_msg}")
        self.connected = False


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Schwab WebSocket Client for streaming market data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python schwab_websocket_client.py
  python schwab_websocket_client.py --symbols my_symbols.txt --options my_options.txt
  python schwab_websocket_client.py --data-dir /path/to/data --debug
        '''
    )
    parser.add_argument('--symbols', '-s', type=str, default='symbols.txt',
                        help='Path to equity symbols file (default: symbols.txt)')
    parser.add_argument('--options', '-o', type=str, default='options_symbols.txt',
                        help='Path to option symbols file (default: options_symbols.txt)')
    parser.add_argument('--token', '-t', type=str, default='schwab_access_token.txt',
                        help='Path to access token file (default: schwab_access_token.txt)')
    parser.add_argument('--data-dir', '-d', type=str, default='data',
                        help='Output directory for CSV files (default: data)')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode')

    args = parser.parse_args()

    # Create client with configurable parameters
    client = SchwabWebSocketClient(
        debug=args.debug,
        symbols_filepath=args.symbols,
        option_symbols_filepath=args.options,
        access_token_filepath=args.token,
        data_output_dir=args.data_dir
    )

    try:
        # Check if it's before market hours and wait until 9:30 AM if needed
        if not client.wait_for_market_open():
            print("❌ Market is closed (weekend). Exiting...")
            exit(0)

        # Connect to WebSocket
        print("🔌 Connecting to Schwab Streaming API...")
        client.connect()

        # Keep the script running until market close
        while client.running and client.connected:
            # Check if it's after market close (4:00:30 PM ET)
            if client.is_after_market_close():
                print("🕐 Market close time (4:00:30 PM ET) reached, disconnecting...")
                client.disconnect()
                break
            time.sleep(1)

        print("✅ Streaming session completed")

    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
        client.disconnect()
    except Exception as e:
        print(f"❌ Error: {e}")
        client.disconnect()
