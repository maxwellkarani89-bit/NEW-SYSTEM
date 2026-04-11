# -*- coding: utf-8 -*-
"""
Edge Finder - Ultimate Trading Analysis Platform
Dark Pro Trader Edition
"""

from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import json
import traceback
import threading
import webbrowser
from functools import wraps
import yfinance as yf
import time
from pathlib import Path
import sqlite3
import hashlib



app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-fallback-key-for-local-only')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///users.db').replace('postgres://', 'postgresql://')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

db = SQLAlchemy(app)

# -----------------------------
# DATABASE MODELS
# -----------------------------
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    favorite_pairs = db.Column(db.Text, default='[]')
    user_settings = db.Column(db.Text, default='{}')
    myfxbook_email = db.Column(db.String(120))
    myfxbook_password = db.Column(db.String(200))

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def get_favorites(self):
        return json.loads(self.favorite_pairs) if self.favorite_pairs else []

    def set_favorites(self, pairs):
        self.favorite_pairs = json.dumps(pairs)

class AnalysisHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    pair = db.Column(db.String(20), nullable=False)
    cot_bias = db.Column(db.String(20))
    cot_momentum = db.Column(db.String(20))
    econ_bias = db.Column(db.String(20))
    econ_diff = db.Column(db.Float)
    trend_score = db.Column(db.Float)
    seasonality_bias = db.Column(db.String(20))
    seasonality_score = db.Column(db.Float)
    overall_score = db.Column(db.Float)
    overall_bias = db.Column(db.String(20))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    user = db.relationship('User', backref=db.backref('histories', lazy=True))

class SignalPerformance(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    pair = db.Column(db.String(20))
    signal_date = db.Column(db.DateTime)
    predicted_bias = db.Column(db.String(20))
    predicted_score = db.Column(db.Float)
    actual_1d_move = db.Column(db.Float)
    actual_1w_move = db.Column(db.Float)
    actual_1m_move = db.Column(db.Float)
    was_correct = db.Column(db.Boolean)
    user = db.relationship('User', backref=db.backref('signal_performance', lazy=True))

class UserNote(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    pair = db.Column(db.String(20))
    note_text = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    user = db.relationship('User', backref=db.backref('notes', lazy=True))

class SeasonalityConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pair = db.Column(db.String(20), nullable=False)
    description = db.Column(db.String(100))  # e.g., "Bullish (70%)"
    start_month = db.Column(db.Integer, nullable=False)
    start_day = db.Column(db.Integer, nullable=False)
    end_month = db.Column(db.Integer, nullable=False)
    end_day = db.Column(db.Integer, nullable=False)
    score = db.Column(db.Float, nullable=False, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# -----------------------------
# DATABASE INITIALIZATION (runs on every startup)
# -----------------------------
with app.app_context():
    db.create_all()
    # Create default admin if missing
    admin = User.query.filter_by(username='Karlmax').first()
    if not admin:
        admin = User(username='Karlmax', email='maxwellkarani89@gmail.com', is_admin=True, is_active=True)
        admin.set_password('admin4125')
        db.session.add(admin)
        db.session.commit()
        print("✅ Database tables created. Admin user ready.")

# -----------------------------
# DATABASE MIGRATION (for SQLite local dev only)
# -----------------------------
def migrate_database():
    db_path = Path.cwd() / 'users.db'
    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [t[0] for t in cursor.fetchall()]
            if 'user' in tables:
                cursor.execute("PRAGMA table_info(user)")
                columns = [col[1] for col in cursor.fetchall()]
                if 'favorite_pairs' not in columns:
                    cursor.execute("ALTER TABLE user ADD COLUMN favorite_pairs TEXT DEFAULT '[]'")
                if 'user_settings' not in columns:
                    cursor.execute("ALTER TABLE user ADD COLUMN user_settings TEXT DEFAULT '{}'")
                if 'is_active' not in columns:
                    cursor.execute("ALTER TABLE user ADD COLUMN is_active BOOLEAN DEFAULT 1")
                conn.commit()
            # Drop trade_journal table if exists (to save space)
            if 'trade_journal' in tables:
                cursor.execute("DROP TABLE trade_journal")
                conn.commit()
                print("Removed trade_journal table.")
            conn.close()
        except Exception as e:
            print(f"Migration error: {e}")

# -----------------------------
# CONFIGURATION
# -----------------------------
UPLOAD_FOLDER = Path.cwd() / 'uploads'
UPLOAD_FOLDER.mkdir(exist_ok=True)

COT_FILE_PATH = str(UPLOAD_FOLDER / "Max_3.xlsx")
ECON_DATA_FILE_PATH = str(UPLOAD_FOLDER / "Economical_analyzer.xlsx")

MAJOR_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD"]

ALL_PAIRS = []
usd_pairs = [("EUR", "USD"), ("GBP", "USD"), ("AUD", "USD"), ("NZD", "USD"),
             ("USD", "JPY"), ("USD", "CAD"), ("USD", "CHF")]
ALL_PAIRS.extend(usd_pairs)
cross_pairs = [("EUR", "GBP"), ("EUR", "JPY"), ("EUR", "AUD"), ("EUR", "CAD"), ("EUR", "CHF"), ("EUR", "NZD"),
               ("GBP", "JPY"), ("GBP", "AUD"), ("GBP", "CAD"), ("GBP", "CHF"), ("GBP", "NZD"),
               ("AUD", "JPY"), ("AUD", "CAD"), ("AUD", "CHF"), ("AUD", "NZD"),
               ("CAD", "JPY"), ("CAD", "CHF"),
               ("CHF", "JPY"),
               ("NZD", "JPY"), ("NZD", "CAD"), ("NZD", "CHF")]
ALL_PAIRS.extend(cross_pairs)

SYMBOL_MAPPING = {
    "NZD/CAD": "NZDCAD=X", "GBP/NZD": "GBPNZD=X", "AUD/CHF": "AUDCHF=X",
    "USD/JPY": "USDJPY=X", "USD/CHF": "USDCHF=X", "USD/CAD": "USDCAD=X",
    "NZD/USD": "NZDUSD=X", "EUR/GBP": "EURGBP=X", "EUR/JPY": "EURJPY=X",
    "GBP/USD": "GBPUSD=X", "EUR/USD": "EURUSD=X", "AUD/USD": "AUDUSD=X",
    "GBP/JPY": "GBPJPY=X", "AUD/JPY": "AUDJPY=X", "CAD/JPY": "CADJPY=X",
    "CHF/JPY": "CHFJPY=X", "NZD/JPY": "NZDJPY=X", "EUR/AUD": "EURAUD=X",
    "EUR/CAD": "EURCAD=X", "EUR/CHF": "EURCHF=X", "EUR/NZD": "EURNZD=X",
    "GBP/AUD": "GBPAUD=X", "GBP/CAD": "GBPCAD=X", "GBP/CHF": "GBPCHF=X",
    "AUD/CAD": "AUDCAD=X", "AUD/NZD": "AUDNZD=X", "CAD/CHF": "CADCHF=X",
    "NZD/CHF": "NZDCHF=X",
}

COLUMN_MAPPING = {
    'long': ['non-commercial longs', 'non commercial longs', 'longs', 'long', 'non-commercial long'],
    'short': ['non-commercial shorts', 'non commercial shorts', 'shorts', 'short', 'non-commercial short'],
    'net': ['net positioning', 'net position', 'net', 'net positioning(longs-shorts)'],
    'change_long': ['change in longs', 'change longs', 'long change', 'change long'],
    'change_short': ['change in shorts', 'change shorts', 'short change', 'change short'],
    'net_change': ['net change', 'change net', 'net change', 'change in net', 'weekly change']
}

LOWER_BETTER_INDICATORS = ["Unemployment Rate", "Jobless Claims", "Claims", "Unemployment Claims"]

cached_analysis = None
cached_heatmap = None
last_analysis_time = None
manual_refresh_triggered = False

# -----------------------------
# AUTHENTICATION
# -----------------------------
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        user = User.query.get(session['user_id'])
        if not user or not user.is_active:
            session.clear()
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        user = User.query.get(session['user_id'])
        if not user or not user.is_admin or not user.is_active:
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated_function

# -----------------------------
# UTILITY FUNCTIONS
# -----------------------------
def safe_convert(value):
    if pd.isna(value) or value == '' or value is None:
        return 0
    try:
        if isinstance(value, (int, float)):
            return float(value)
        str_val = str(value).strip().replace(',', '').replace('$', '').replace('%', '').replace('K', '000').replace('M', '000000')
        return float(str_val) if str_val else 0
    except:
        return 0

def get_bias(value):
    if value > 0: return "Bullish"
    elif value < 0: return "Bearish"
    return "Neutral"

def get_symbol(value):
    if value > 0: return "▲"
    elif value < 0: return "▼"
    return "●"

def convert_to_serializable(obj):
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_to_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    else:
        return obj

# -----------------------------
# TREND ANALYSIS
# -----------------------------
def get_trend_score(pair_symbol):
    try:
        if not pair_symbol.endswith('=X'):
            pair_symbol = pair_symbol + '=X'
        ticker = yf.Ticker(pair_symbol)
        hist = ticker.history(period="6mo", interval="1d")
        if len(hist) < 100:
            return 0
        sma_100 = hist['Close'].rolling(window=100).mean().iloc[-1]
        current_price = hist['Close'].iloc[-1]
        if current_price > sma_100:
            return 1
        elif current_price < sma_100:
            return -1
        return 0
    except:
        return 0

# -----------------------------
# SEASONALITY ANALYSIS (DB-driven)
# -----------------------------
def get_seasonality_bias(pair):
    today = datetime.now()
    current_month = today.month
    current_day = today.day

    configs = SeasonalityConfig.query.filter_by(pair=pair).all()
    for cfg in configs:
        start = (cfg.start_month, cfg.start_day)
        end = (cfg.end_month, cfg.end_day)
        current = (current_month, current_day)
        if start <= end:
            in_range = start <= current <= end
        else:
            in_range = current >= start or current <= end
        if in_range:
            return cfg.description, cfg.score
    return "Neutral", 0

# -----------------------------
# COT ANALYSIS
# -----------------------------
def find_column(cols, names):
    for col in cols:
        col_lower = str(col).lower().strip()
        for name in names:
            if name.lower() in col_lower:
                return col
    return None

def get_cot_metrics(df):
    df = df.dropna(how='all')
    if df.empty:
        return 0, 0
    df.columns = [str(c).strip() if pd.notna(c) else f"Unnamed_{i}" for i, c in enumerate(df.columns)]
    long_col = find_column(df.columns, COLUMN_MAPPING['long'])
    short_col = find_column(df.columns, COLUMN_MAPPING['short'])
    net_col = find_column(df.columns, COLUMN_MAPPING['net'])
    change_col = find_column(df.columns, COLUMN_MAPPING['net_change'])
    if not any([long_col, short_col, net_col]):
        return 0, 0
    valid_rows = df.dropna(how='all')
    if len(valid_rows) == 0:
        return 0, 0
    last_row = valid_rows.iloc[-1]
    if net_col and net_col in last_row.index:
        net = safe_convert(last_row[net_col])
    elif long_col and short_col:
        long_val = safe_convert(last_row[long_col]) if long_col in last_row.index else 0
        short_val = safe_convert(last_row[short_col]) if short_col in last_row.index else 0
        net = long_val - short_val
    else:
        net = 0
    if change_col and change_col in last_row.index:
        net_change = safe_convert(last_row[change_col])
    else:
        net_change = 0
    return net, net_change

def analyze_cot(pairs):
    try:
        if not os.path.exists(COT_FILE_PATH):
            return pd.DataFrame(), {}
        xls = pd.ExcelFile(COT_FILE_PATH)
    except Exception:
        return pd.DataFrame(), {}

    currency_data = {}
    for sheet in xls.sheet_names:
        try:
            df_raw = pd.read_excel(xls, sheet_name=sheet, header=None)
            if df_raw.empty:
                continue
            header_row = None
            for i in range(min(20, len(df_raw))):
                row_vals = df_raw.iloc[i].astype(str).str.lower()
                if any('non-commercial' in v for v in row_vals) or (any('long' in v for v in row_vals) and any('short' in v for v in row_vals)):
                    header_row = i
                    break
            if header_row is not None:
                df = pd.read_excel(xls, sheet_name=sheet, header=header_row)
            else:
                df = pd.read_excel(xls, sheet_name=sheet, header=0)
            if df.empty:
                continue
            net, change = get_cot_metrics(df)
            clean_sheet = sheet.strip().upper()
            currency_data[clean_sheet] = {"net": net, "change": change}
        except Exception:
            continue

    results = []
    for base, quote in pairs:
        base_upper = base.upper()
        quote_upper = quote.upper()
        if base_upper not in currency_data or quote_upper not in currency_data:
            continue
        base_net = currency_data[base_upper]["net"]
        quote_net = currency_data[quote_upper]["net"]
        pair_net = base_net - quote_net
        pair_change = currency_data[base_upper]["change"] - currency_data[quote_upper]["change"]
        cot_score = 1 if pair_net > 0 else (-1 if pair_net < 0 else 0)
        momentum_score = 1 if pair_change > 0 else (-1 if pair_change < 0 else 0)
        results.append({
            "Pair": f"{base}/{quote}",
            "COT_Bias": get_bias(pair_net),
            "COT_Symbol": get_symbol(pair_net),
            "Momentum": get_bias(pair_change),
            "Mom_Symbol": get_symbol(pair_change),
            "COT_Score": cot_score,
            "Momentum_Score": momentum_score
        })
    return pd.DataFrame(results), currency_data

# -----------------------------
# ECONOMIC ANALYSIS
# -----------------------------
def is_lower_better(name):
    return any(k.lower() in str(name).lower() for k in LOWER_BETTER_INDICATORS)

def analyze_currency_econ(currency):
    try:
        if not os.path.exists(ECON_DATA_FILE_PATH):
            return None
        df = pd.read_excel(ECON_DATA_FILE_PATH, sheet_name=currency)
    except Exception:
        return None
    if df.empty:
        return None
    df.columns = [str(c).strip().lower() for c in df.columns]
    ind_col = next((c for c in df.columns if 'economic' in c or 'indicator' in c or 'event' in c), None)
    fcast_col = next((c for c in df.columns if 'forecast' in c or 'expected' in c or 'consensus' in c), None)
    actual_col = next((c for c in df.columns if 'actual' in c or 'released' in c), None)
    if not ind_col:
        ind_col = df.columns[0] if len(df.columns) > 0 else None
    if not actual_col:
        for col in df.columns:
            if 'actual' in col or 'released' in col:
                actual_col = col
                break
    if not ind_col or not actual_col:
        return 50
    bullish = bearish = 0
    for _, row in df.iterrows():
        name = row[ind_col]
        if pd.isna(name):
            continue
        if "Employment Change" in str(name):
            continue
        forecast = safe_convert(row[fcast_col]) if fcast_col else 0
        actual = safe_convert(row[actual_col]) if actual_col else 0
        if forecast == 0 and actual == 0:
            continue
        if is_lower_better(name):
            if actual < forecast:
                bullish += 1
            elif actual > forecast:
                bearish += 1
        else:
            if actual > forecast:
                bullish += 1
            elif actual < forecast:
                bearish += 1
    total = bullish + bearish
    if total == 0:
        return 50
    return (bullish / total) * 100

def get_econ_pair_bias(base, quote):
    base_pct = analyze_currency_econ(base)
    quote_pct = analyze_currency_econ(quote)
    if base_pct is None and quote_pct is None:
        return None, None, 0, 0
    base_pct = base_pct if base_pct is not None else 50
    quote_pct = quote_pct if quote_pct is not None else 50
    diff_pct = base_pct - quote_pct
    abs_diff = abs(diff_pct)
    if abs_diff > 60:
        econ_score = 4
    elif abs_diff > 40:
        econ_score = 3
    elif abs_diff > 20:
        econ_score = 2
    elif abs_diff > 0:
        econ_score = 1
    else:
        econ_score = 0
    if diff_pct < 0:
        econ_score = -econ_score
    if diff_pct > 60:
        econ_bias, econ_sym = "EXTREME BULLISH", "▲▲▲"
    elif diff_pct > 40:
        econ_bias, econ_sym = "VERY BULLISH", "▲▲"
    elif diff_pct > 20:
        econ_bias, econ_sym = "BULLISH", "▲"
    elif diff_pct > 0:
        econ_bias, econ_sym = "SLIGHTLY BULLISH", "▲"
    elif diff_pct > -20:
        econ_bias, econ_sym = "SLIGHTLY BEARISH", "▼"
    elif diff_pct > -40:
        econ_bias, econ_sym = "BEARISH", "▼"
    elif diff_pct > -60:
        econ_bias, econ_sym = "VERY BEARISH", "▼▼"
    else:
        econ_bias, econ_sym = "EXTREME BEARISH", "▼▼▼"
    return econ_bias, econ_sym, diff_pct, econ_score

# -----------------------------
# SCORING FUNCTION
# -----------------------------
def get_overall_bias_and_color(score):
    if score >= 8: return "EXTREME BUY", "🔥🔥🔥", "#00e5a0", score
    elif score >= 6: return "STRONG BUY", "🔥🔥", "#00cc88", score
    elif score >= 4: return "BUY", "🔥", "#00aa66", score
    elif score >= 2: return "MODERATE BUY", "▲", "#66ffb3", score
    elif score >= 1: return "SLIGHT BUY", "▲", "#99ffcc", score
    elif score <= -8: return "EXTREME SELL", "💀💀💀", "#ff4d6d", score
    elif score <= -6: return "STRONG SELL", "💀💀", "#ff6688", score
    elif score <= -4: return "SELL", "💀", "#ff8099", score
    elif score <= -2: return "MODERATE SELL", "▼", "#ffb3c1", score
    elif score <= -1: return "SLIGHT SELL", "▼", "#ffe6ea", score
    else: return "NEUTRAL", "⚡", "#ffaa00", score

def calculate_currency_strength(results):
    currency_scores = {curr: 0 for curr in MAJOR_CURRENCIES}
    currency_counts = {curr: 0 for curr in MAJOR_CURRENCIES}
    currency_econ_data = {}
    for curr in MAJOR_CURRENCIES:
        econ_pct = analyze_currency_econ(curr)
        currency_econ_data[curr] = econ_pct if econ_pct is not None else 50
    for result in results:
        if 'pair' not in result or 'overall' not in result:
            continue
        pair = result['pair']
        base, quote = pair.split('/')
        overall_score = result['overall']['score']
        currency_scores[base] += overall_score
        currency_scores[quote] -= overall_score
        currency_counts[base] += 1
        currency_counts[quote] += 1
    for curr in currency_scores:
        if currency_counts[curr] > 0:
            currency_scores[curr] = currency_scores[curr] / currency_counts[curr]
    sorted_currencies = sorted(currency_scores.items(), key=lambda x: x[1], reverse=True)
    return sorted_currencies, currency_scores, currency_econ_data

# -----------------------------
# API ROUTES
# -----------------------------
@app.route('/api/analyze', methods=['POST'])
@login_required
def api_analyze():
    global cached_analysis, cached_heatmap, last_analysis_time, manual_refresh_triggered
    try:
        if not os.path.exists(COT_FILE_PATH):
            return jsonify({'error': 'No COT data file found'}), 400
        if not os.path.exists(ECON_DATA_FILE_PATH):
            return jsonify({'error': 'No Economic data file found'}), 400

        use_cache = False
        if not manual_refresh_triggered and cached_analysis is not None and last_analysis_time is not None:
            time_diff = (datetime.now() - last_analysis_time).total_seconds()
            if time_diff < 300:
                use_cache = True

        if use_cache:
            return jsonify({'results': cached_analysis, 'heatmap': cached_heatmap, 'cached': True})

        manual_refresh_triggered = False
        user = User.query.get(session['user_id'])
        cot_df, cot_raw = analyze_cot(ALL_PAIRS)
        results = []

        for base, quote in ALL_PAIRS:
            pair_name = f"{base}/{quote}"
            if 'Pair' not in cot_df.columns:
                cot_row = pd.DataFrame()
            else:
                cot_row = cot_df[cot_df['Pair'] == pair_name]
            if not cot_row.empty:
                r = cot_row.iloc[0]
                cot_data = {'bias': str(r['COT_Bias']), 'symbol': str(r['COT_Symbol']), 'momentum': str(r['Momentum']), 'mom_symbol': str(r['Mom_Symbol']), 'score': int(r['COT_Score']), 'momentum_score': int(r['Momentum_Score'])}
                cot_score, momentum_score = cot_data['score'], cot_data['momentum_score']
            else:
                cot_data = {'bias': 'No Data', 'symbol': '●', 'momentum': 'No Data', 'mom_symbol': '●', 'score': 0, 'momentum_score': 0}
                cot_score = momentum_score = 0

            econ_bias, econ_sym, diff_pct, econ_score = get_econ_pair_bias(base, quote)
            if econ_bias:
                econ_data = {'bias': str(econ_bias), 'symbol': str(econ_sym), 'diff': float(diff_pct), 'score': int(econ_score)}
            else:
                econ_data = {'bias': 'No Data', 'symbol': '●', 'diff': 0, 'score': 0}
                econ_score = 0

            yf_symbol = SYMBOL_MAPPING.get(pair_name, pair_name.replace('/', '') + '=X')
            trend_score = get_trend_score(yf_symbol)
            trend_bias = "Bullish" if trend_score > 0 else "Bearish" if trend_score < 0 else "Neutral"
            trend_symbol = "▲" if trend_score > 0 else "▼" if trend_score < 0 else "●"

            seasonality_bias, seasonality_score = get_seasonality_bias(pair_name)

            overall_score = cot_score + momentum_score + econ_score + trend_score + seasonality_score
            overall_bias, overall_symbol, overall_color, display_score = get_overall_bias_and_color(overall_score)

            result = {
                'pair': str(pair_name),
                'cot': cot_data,
                'economic': econ_data,
                'trend': {'bias': trend_bias, 'symbol': trend_symbol, 'score': trend_score},
                'seasonality': {'bias': seasonality_bias, 'score': seasonality_score},
                'overall': {'bias': overall_bias, 'symbol': overall_symbol, 'score': display_score, 'color': overall_color},
            }
            results.append(result)

            history = AnalysisHistory(user_id=user.id, pair=str(pair_name), cot_bias=str(cot_data['bias']), cot_momentum=str(cot_data['momentum']), econ_bias=str(econ_data['bias']), econ_diff=float(econ_data.get('diff', 0)), trend_score=float(trend_score), seasonality_bias=str(seasonality_bias), seasonality_score=float(seasonality_score), overall_score=float(overall_score), overall_bias=str(overall_bias))
            db.session.add(history)

        db.session.commit()
        currency_ranking, currency_scores, currency_econ_data = calculate_currency_strength(results)

        serializable_results = convert_to_serializable(results)
        serializable_ranking = convert_to_serializable(currency_ranking)
        serializable_econ = convert_to_serializable(currency_econ_data)
        heatmap_data = {'ranking': serializable_ranking, 'econ_data': serializable_econ}
        cached_analysis = serializable_results
        cached_heatmap = heatmap_data
        last_analysis_time = datetime.now()
        return jsonify({'results': serializable_results, 'heatmap': heatmap_data, 'cached': False})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/pair_detail/<pair>')
@login_required
def api_pair_detail(pair):
    """Return detailed COT and economic data for base and quote currencies of a pair."""
    try:
        base, quote = pair.split('/')
        
        cot_raw = {}
        try:
            _, cot_raw = analyze_cot(ALL_PAIRS)
        except Exception as e:
            print(f"COT analysis failed for detail view: {e}")
            cot_raw = {}
        
        base_cot = cot_raw.get(base.upper(), {"net": 0, "change": 0})
        quote_cot = cot_raw.get(quote.upper(), {"net": 0, "change": 0})
        
        base_econ = analyze_currency_econ(base)
        if base_econ is None:
            base_econ = 50
        quote_econ = analyze_currency_econ(quote)
        if quote_econ is None:
            quote_econ = 50
        
        return jsonify({
            'pair': pair,
            'base': {
                'currency': base,
                'cot_net': base_cot.get('net', 0),
                'cot_change': base_cot.get('change', 0),
                'econ_pct': round(base_econ, 1)
            },
            'quote': {
                'currency': quote,
                'cot_net': quote_cot.get('net', 0),
                'cot_change': quote_cot.get('change', 0),
                'econ_pct': round(quote_econ, 1)
            }
        })
    except Exception as e:
        print(f"Pair detail error: {e}")
        base, quote = pair.split('/') if '/' in pair else ('???', '???')
        return jsonify({
            'pair': pair,
            'base': {'currency': base, 'cot_net': 0, 'cot_change': 0, 'econ_pct': 50},
            'quote': {'currency': quote, 'cot_net': 0, 'cot_change': 0, 'econ_pct': 50},
            'error': 'Some data unavailable'
        })

# -----------------------------
# CURRENCY DATA API
# -----------------------------
@app.route('/api/currencies')
@login_required
def api_currencies():
    """Return economic sentiment and COT data for all major currencies."""
    try:
        cot_raw = {}
        cot_last_updated = None
        if os.path.exists(COT_FILE_PATH):
            cot_last_updated = datetime.fromtimestamp(os.path.getmtime(COT_FILE_PATH))
            try:
                _, cot_raw = analyze_cot(ALL_PAIRS)
            except Exception as e:
                print(f"COT analysis failed for currencies view: {e}")

        econ_last_updated = None
        if os.path.exists(ECON_DATA_FILE_PATH):
            econ_last_updated = datetime.fromtimestamp(os.path.getmtime(ECON_DATA_FILE_PATH))

        currencies = []
        for curr in MAJOR_CURRENCIES:
            econ_pct = analyze_currency_econ(curr)
            if econ_pct is None:
                econ_pct = 50

            cot_data = cot_raw.get(curr.upper(), {"net": 0, "change": 0})
            cot_net = cot_data.get('net', 0)
            cot_change = cot_data.get('change', 0)

            currencies.append({
                'currency': curr,
                'econ_pct': round(econ_pct, 1),
                'cot_net': cot_net,
                'cot_change': cot_change
            })

        return jsonify({
            'currencies': currencies,
            'cot_last_updated': cot_last_updated.isoformat() if cot_last_updated else None,
            'econ_last_updated': econ_last_updated.isoformat() if econ_last_updated else None
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# -----------------------------
# USER SETTINGS ROUTES
# -----------------------------
@app.route('/api/favorites', methods=['GET', 'POST'])
@login_required
def api_favorites():
    user = User.query.get(session['user_id'])
    if request.method == 'POST':
        data = request.json
        user.set_favorites(data.get('favorites', []))
        db.session.commit()
        return jsonify({'success': True})
    return jsonify({'favorites': user.get_favorites()})

@app.route('/api/notes/<pair>', methods=['GET', 'POST', 'DELETE'])
@login_required
def api_notes(pair):
    user = User.query.get(session['user_id'])
    if request.method == 'POST':
        data = request.json
        note = UserNote.query.filter_by(user_id=user.id, pair=pair).first()
        if note:
            note.note_text = data.get('note', '')
            note.updated_at = datetime.utcnow()
        else:
            note = UserNote(user_id=user.id, pair=pair, note_text=data.get('note', ''))
            db.session.add(note)
        db.session.commit()
        return jsonify({'success': True})
    elif request.method == 'DELETE':
        UserNote.query.filter_by(user_id=user.id, pair=pair).delete()
        db.session.commit()
        return jsonify({'success': True})
    else:
        note = UserNote.query.filter_by(user_id=user.id, pair=pair).first()
        return jsonify({'note': note.note_text if note else ''})

# -----------------------------
# ADMIN ROUTES
# -----------------------------
@app.route('/admin')
@login_required
@admin_required
def admin_panel():
    return render_template('admin.html', username=session['username'], ALL_PAIRS=ALL_PAIRS)

@app.route('/admin/refresh', methods=['POST'])
@login_required
@admin_required
def admin_refresh():
    global cached_analysis, cached_heatmap, last_analysis_time, manual_refresh_triggered
    cached_analysis = None
    cached_heatmap = None
    last_analysis_time = None
    manual_refresh_triggered = True
    return jsonify({'success': True})

@app.route('/admin/check-files', methods=['GET'])
@login_required
@admin_required
def check_files():
    return jsonify({
        'cot_file': {'exists': os.path.exists(COT_FILE_PATH)},
        'econ_file': {'exists': os.path.exists(ECON_DATA_FILE_PATH)}
    })

@app.route('/admin/cot/upload', methods=['POST'])
@login_required
@admin_required
def admin_cot_upload():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        file_path = str(UPLOAD_FOLDER / "Max_3.xlsx")
        if os.path.exists(file_path):
            os.remove(file_path)
        file.save(file_path)
        global cached_analysis, cached_heatmap, last_analysis_time, manual_refresh_triggered
        cached_analysis = None
        cached_heatmap = None
        last_analysis_time = None
        manual_refresh_triggered = True
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/admin/econ/upload', methods=['POST'])
@login_required
@admin_required
def admin_econ_upload():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        file_path = str(UPLOAD_FOLDER / "Economical_analyzer.xlsx")
        if os.path.exists(file_path):
            os.remove(file_path)
        file.save(file_path)
        global cached_analysis, cached_heatmap, last_analysis_time, manual_refresh_triggered
        cached_analysis = None
        cached_heatmap = None
        last_analysis_time = None
        manual_refresh_triggered = True
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/admin/users', methods=['GET'])
@login_required
@admin_required
def admin_users():
    users = User.query.all()
    return jsonify({'users': [{'id': u.id, 'username': u.username, 'email': u.email, 'is_admin': u.is_admin, 'is_active': u.is_active} for u in users]})

@app.route('/admin/users/<int:user_id>/activate', methods=['POST'])
@login_required
@admin_required
def admin_activate_user(user_id):
    user = User.query.get_or_404(user_id)
    user.is_active = True
    db.session.commit()
    return jsonify({'success': True})

@app.route('/admin/users/<int:user_id>/deactivate', methods=['POST'])
@login_required
@admin_required
def admin_deactivate_user(user_id):
    user = User.query.get_or_404(user_id)
    user.is_active = False
    db.session.commit()
    return jsonify({'success': True})

# -----------------------------
# SEASONALITY ADMIN ROUTES
# -----------------------------
@app.route('/admin/seasonality', methods=['GET'])
@login_required
@admin_required
def admin_seasonality_list():
    configs = SeasonalityConfig.query.order_by(SeasonalityConfig.pair).all()
    return jsonify([{
        'id': c.id,
        'pair': c.pair,
        'description': c.description,
        'start_month': c.start_month,
        'start_day': c.start_day,
        'end_month': c.end_month,
        'end_day': c.end_day,
        'score': c.score
    } for c in configs])

@app.route('/admin/seasonality', methods=['POST'])
@login_required
@admin_required
def admin_seasonality_create():
    data = request.json
    cfg = SeasonalityConfig(
        pair=data['pair'],
        description=data['description'],
        start_month=int(data['start_month']),
        start_day=int(data['start_day']),
        end_month=int(data['end_month']),
        end_day=int(data['end_day']),
        score=float(data['score'])
    )
    db.session.add(cfg)
    db.session.commit()
    return jsonify({'success': True, 'id': cfg.id})

@app.route('/admin/seasonality/<int:cfg_id>', methods=['PUT', 'DELETE'])
@login_required
@admin_required
def admin_seasonality_modify(cfg_id):
    cfg = SeasonalityConfig.query.get_or_404(cfg_id)
    if request.method == 'DELETE':
        db.session.delete(cfg)
        db.session.commit()
        return jsonify({'success': True})
    else:
        data = request.json
        cfg.pair = data.get('pair', cfg.pair)
        cfg.description = data.get('description', cfg.description)
        cfg.start_month = int(data.get('start_month', cfg.start_month))
        cfg.start_day = int(data.get('start_day', cfg.start_day))
        cfg.end_month = int(data.get('end_month', cfg.end_month))
        cfg.end_day = int(data.get('end_day', cfg.end_day))
        cfg.score = float(data.get('score', cfg.score))
        db.session.commit()
        return jsonify({'success': True})

# -----------------------------
# MAIN ROUTES
# -----------------------------
@app.route('/')
def index():
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = User.query.filter_by(username=username).first()
        if user and user.check_password(password):
            if not user.is_active:
                return render_template('login.html', error='Your account is pending admin approval.')
            session['user_id'] = user.id
            session['username'] = user.username
            session['is_admin'] = user.is_admin
            return redirect(url_for('dashboard'))
        return render_template('login.html', error='Invalid credentials')
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        confirm = request.form['confirm_password']
        if password != confirm:
            return render_template('register.html', error='Passwords do not match')
        if User.query.filter_by(username=username).first():
            return render_template('register.html', error='Username exists')
        if User.query.filter_by(email=email).first():
            return render_template('register.html', error='Email exists')
        user = User(username=username, email=email, is_active=False)  # Require admin approval
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    user = User.query.get(session['user_id'])
    return render_template('dashboard.html', username=session['username'], is_admin=session.get('is_admin', False), favorites=user.get_favorites())

@app.route('/currencies')
@login_required
def currencies():
    return render_template('currencies.html', username=session['username'], is_admin=session.get('is_admin', False))

@app.route('/api/history')
@login_required
def api_history():
    histories = AnalysisHistory.query.filter_by(user_id=session['user_id']).order_by(AnalysisHistory.created_at.desc()).limit(50).all()
    return jsonify([{'date': h.created_at.strftime('%Y-%m-%d %H:%M:%S'), 'pair': h.pair, 'cot_bias': h.cot_bias, 'econ_bias': h.econ_bias, 'trend_score': h.trend_score, 'seasonality_bias': h.seasonality_bias, 'overall_bias': h.overall_bias} for h in histories])

@app.route('/profile')
@login_required
def profile():
    return render_template('profile.html', username=session['username'])

# -----------------------------
# CREATE TEMPLATES
# -----------------------------
def create_templates():
    os.makedirs('templates', exist_ok=True)

    # Login template (unchanged except for error message wording)
    with open('templates/login.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tradion · Login</title>
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body { font-family:'Segoe UI','Inter',sans-serif; background:#0B0F1A; color:#E0E0E0; height:100vh; display:flex; overflow:hidden; }
        .split-container { display:flex; width:100%; }
        .brand-panel { flex:1.2; background:radial-gradient(circle at 20% 30%, #121826 0%, #0B0F1A 100%); display:flex; flex-direction:column; justify-content:center; align-items:center; padding:3rem; position:relative; overflow:hidden; }
        .brand-content { max-width:500px; z-index:2; text-align:center; }
        .logo { font-size:3.8rem; font-weight:800; letter-spacing:4px; background:linear-gradient(135deg, #00e5ff 0%, #00b8d4 80%); -webkit-background-clip:text; background-clip:text; color:transparent; margin-bottom:1rem; text-shadow:0 0 30px rgba(0,229,255,0.3); }
        .tagline { font-size:1.4rem; color:#a0b0c0; margin-bottom:3rem; font-weight:300; letter-spacing:1px; }
        .chart-animation { width:100%; height:200px; margin-top:2rem; opacity:0.6; }
        .chart-line { stroke:#00e5ff; stroke-width:2; fill:none; stroke-dasharray:1000; stroke-dashoffset:1000; animation:drawLine 4s ease-out forwards; }
        @keyframes drawLine { to { stroke-dashoffset:0; } }
        .glow-pulse { position:absolute; width:300px; height:300px; background:radial-gradient(circle, rgba(0,229,255,0.15) 0%, transparent 70%); border-radius:50%; top:20%; left:30%; animation:pulse 8s infinite alternate; z-index:1; }
        @keyframes pulse { 0% { transform:scale(1); opacity:0.3; } 100% { transform:scale(1.5); opacity:0.1; } }
        .login-panel { flex:0.8; background:#0B0F1A; display:flex; align-items:center; justify-content:center; padding:2rem; }
        .login-card { background:#121826; border-radius:20px; padding:2.5rem 2rem; width:100%; max-width:420px; box-shadow:0 20px 40px rgba(0,0,0,0.6), 0 0 0 1px rgba(0,229,255,0.1); border:1px solid #2a3040; }
        .login-card h2 { font-size:2rem; font-weight:600; color:#FFFFFF; margin-bottom:0.5rem; }
        .subtitle { color:#8892b0; margin-bottom:2rem; font-size:0.95rem; }
        .input-group { margin-bottom:1.5rem; }
        .input-group label { display:block; margin-bottom:0.5rem; color:#ccd6f6; font-size:0.9rem; font-weight:500; }
        .input-wrapper { position:relative; }
        .input-wrapper input { width:100%; padding:0.9rem 1rem; background:#1a1f2e; border:1.5px solid #2a3040; border-radius:12px; color:#fff; font-size:1rem; transition:all 0.2s; }
        .input-wrapper input:focus { outline:none; border-color:#00e5ff; box-shadow:0 0 0 3px rgba(0,229,255,0.1); }
        .input-wrapper input::placeholder { color:#5a6380; }
        .password-toggle { position:absolute; right:15px; top:50%; transform:translateY(-50%); color:#8892b0; cursor:pointer; font-size:1.2rem; }
        .btn-login { width:100%; padding:0.9rem; background:linear-gradient(135deg, #00e5ff 0%, #00b8d4 100%); border:none; border-radius:12px; color:#0B0F1A; font-weight:700; font-size:1.1rem; cursor:pointer; transition:all 0.2s; margin-top:0.5rem; position:relative; overflow:hidden; }
        .btn-login:hover { transform:translateY(-2px); box-shadow:0 10px 20px rgba(0,229,255,0.2); }
        .btn-login:active { transform:translateY(0); }
        .btn-login::after { content:''; position:absolute; top:50%; left:50%; width:5px; height:5px; background:rgba(255,255,255,0.5); opacity:0; border-radius:100%; transform:scale(1,1) translate(-50%); transform-origin:50% 50%; }
        .btn-login:focus:not(:active)::after { animation:ripple 1s ease-out; }
        @keyframes ripple { 0% { transform:scale(0,0); opacity:0.5; } 100% { transform:scale(20,20); opacity:0; } }
        .error-message { background:rgba(255,77,109,0.15); color:#ff8099; padding:0.8rem; border-radius:10px; margin-bottom:1rem; font-size:0.9rem; border-left:4px solid #ff4d6d; animation:shake 0.3s ease-in-out; }
        @keyframes shake { 0%,100%{ transform:translateX(0); } 25%{ transform:translateX(-5px); } 75%{ transform:translateX(5px); } }
        .footer-text { text-align:center; margin-top:1.5rem; color:#8892b0; font-size:0.9rem; }
        .footer-text a { color:#00e5ff; text-decoration:none; font-weight:600; }
        .footer-text a:hover { text-decoration:underline; }
        @media (max-width:768px) { .split-container { flex-direction:column; } .brand-panel { display:none; } }
    </style>
</head>
<body>
    <div class="split-container">
        <div class="brand-panel">
            <div class="glow-pulse"></div>
            <div class="brand-content">
                <div class="logo">Tradion</div>
                <div class="tagline">Trade smarter. Analyze deeper.</div>
                <svg class="chart-animation" viewBox="0 0 400 100" preserveAspectRatio="none">
                    <polyline class="chart-line" points="0,80 50,60 100,70 150,20 200,40 250,10 300,50 350,30 400,45" />
                </svg>
            </div>
        </div>
        <div class="login-panel">
            <div class="login-card">
                <h2>Welcome Back</h2>
                <p class="subtitle">Access your trading dashboard</p>
                {% if error %}<div class="error-message">{{ error }}</div>{% endif %}
                <form method="POST">
                    <div class="input-group">
                        <label>Username</label>
                        <div class="input-wrapper">
                            <input type="text" name="username" placeholder="Enter username" required autofocus>
                        </div>
                    </div>
                    <div class="input-group">
                        <label>Password</label>
                        <div class="input-wrapper">
                            <input type="password" name="password" id="password" placeholder="••••••••" required>
                            <span class="password-toggle" onclick="togglePassword()">👁</span>
                        </div>
                    </div>
                    <button type="submit" class="btn-login">Sign In</button>
                </form>
                <p class="footer-text">Don't have an account? <a href="{{ url_for('register') }}">Sign up</a></p>
            </div>
        </div>
    </div>
    <script>
        function togglePassword() {
            const pwd = document.getElementById('password');
            const toggle = document.querySelector('.password-toggle');
            if (pwd.type === 'password') {
                pwd.type = 'text';
                toggle.textContent = '🙈';
            } else {
                pwd.type = 'password';
                toggle.textContent = '👁';
            }
        }
    </script>
</body>
</html>''')

    # Register template
    with open('templates/register.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html>
<head><title>Register - Tradion</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',sans-serif;background:#0B0F1A;min-height:100vh;display:flex;justify-content:center;align-items:center}
.register-container{background:#121826;border-radius:20px;padding:40px;width:100%;max-width:400px;box-shadow:0 0 40px rgba(0,229,255,0.1);border:1px solid #2a3040}
h2{color:#00e5ff;text-align:center;margin-bottom:30px}
input{width:100%;padding:12px;margin:10px 0;border:none;border-radius:10px;background:#1a1f2e;color:#fff;border:1.5px solid #2a3040}
input:focus{border-color:#00e5ff;outline:none}
button{width:100%;padding:12px;background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;border:none;border-radius:10px;cursor:pointer;font-weight:bold}
.error{color:#ff8099;text-align:center;margin-top:10px}
.link{text-align:center;margin-top:20px}
.link a{color:#00e5ff}
</style>
</head>
<body>
<div class="register-container">
<h2>Create Account</h2>
<form method="POST">
<input type="text" name="username" placeholder="Username" required>
<input type="email" name="email" placeholder="Email" required>
<input type="password" name="password" placeholder="Password" required>
<input type="password" name="confirm_password" placeholder="Confirm Password" required>
<button type="submit">Register</button>
</form>
{% if error %}<div class="error">{{ error }}</div>{% endif %}
<div class="link"><a href="{{ url_for('login') }}">Back to Login</a></div>
</div>
</body>
</html>''')

    # Dashboard template (collapsible sidebar + active highlight fix)
    with open('templates/dashboard.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html>
<head>
    <title>Tradion · Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;display:flex}
        .sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100;transition:width 0.3s}
        .sidebar.collapsed{width:80px}
        .sidebar .logo{font-size:24px;font-weight:800;color:#00e5ff;text-align:center;margin-bottom:30px;padding-bottom:20px;border-bottom:1px solid #2a3040;display:flex;align-items:center;justify-content:center}
        .sidebar.collapsed .logo{font-size:20px}
        .sidebar.collapsed .logo span{display:none}
        .sidebar .menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:10px;cursor:pointer;transition:all 0.2s;color:#a0b0c0}
        .sidebar .menu-item:hover{background:rgba(0,229,255,0.08);color:#00e5ff}
        .sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;font-weight:bold}
        .sidebar .menu-icon{font-size:20px;margin-right:12px}
        .sidebar.collapsed .menu-icon{margin-right:0}
        .sidebar.collapsed .menu-item span:not(.menu-icon){display:none}
        .main-content{flex:1;margin-left:280px;padding:20px 30px;transition:margin-left 0.3s}
        .navbar{background:#121826;padding:15px 25px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #2a3040;border-radius:0 0 16px 16px;margin-bottom:20px}
        .navbar-title{font-size:18px;color:#00e5ff}
        button{padding:10px 20px;background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;border:none;border-radius:8px;cursor:pointer;font-weight:bold;transition:all 0.2s}
        button:hover{transform:scale(1.02);box-shadow:0 0 15px rgba(0,229,255,0.3)}
        button.secondary{background:transparent;border:1px solid #00e5ff;color:#00e5ff}
        table{width:100%;border-collapse:collapse;margin-top:20px;background:#121826;font-size:13px;border-radius:12px;overflow:hidden}
        th,td{padding:12px 8px;text-align:center;border-bottom:1px solid #2a3040}
        th{background:rgba(0,229,255,0.1);color:#00e5ff;font-weight:600}
        .currency-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:20px;margin-top:20px}
        .currency-card{background:#121826;border-radius:16px;padding:20px 15px;text-align:center;transition:all 0.2s;border:1px solid #2a3040;cursor:pointer;position:relative}
        .currency-card:hover{transform:translateY(-5px);border-color:#00e5ff;box-shadow:0 10px 25px rgba(0,229,255,0.1)}
        .favorite-star{position:absolute;top:10px;right:10px;font-size:20px;cursor:pointer;color:#ffb800;opacity:0.6;transition:all 0.2s}
        .favorite-star.active{opacity:1;text-shadow:0 0 10px #ffb800}
        .gauge-wrapper{position:relative;width:120px;height:120px;margin:15px auto}
        .gauge-svg{transform:rotate(-90deg);width:100%;height:100%}
        .gauge-bg-circle{stroke:#2a3040;stroke-width:10;fill:none}
        .gauge-fill-circle{stroke-width:10;fill:none;stroke-linecap:round;transition:stroke-dasharray 0.8s}
        .gauge-center{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center}
        .gauge-value{font-size:22px;font-weight:bold;color:#00e5ff}
        .content-pane{display:none}
        .content-pane.active{display:block}
        .modal{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.7);z-index:1000;justify-content:center;align-items:center}
        .modal-content{background:#121826;padding:25px;border-radius:16px;width:90%;max-width:500px;border:1px solid #2a3040}
        .loading-skeleton{background:linear-gradient(90deg,#1a1f2e,#2a3040,#1a1f2e);background-size:200% 100%;animation:shimmer 1.5s infinite}
        @keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
        .search-box{width:100%;max-width:300px;padding:10px 15px;background:#1a1f2e;border:1px solid #2a3040;border-radius:8px;color:#fff;margin-bottom:15px}
        .clickable-pair{cursor:pointer;text-decoration:underline dotted #00e5ff}
        .detail-row{display:flex;justify-content:space-between;margin-bottom:8px}
        .toggle-btn{background:none;border:none;color:#00e5ff;font-size:24px;cursor:pointer;margin-right:10px}
    </style>
</head>
<body>
<div class="sidebar" id="sidebar">
    <div class="logo">
        <button class="toggle-btn" onclick="toggleSidebar()">☰</button>
        <span>⚡ Tradion</span>
    </div>
    <div class="menu-item active" onclick="showPane('analysis')"><span class="menu-icon">📊</span><span>Analysis</span></div>
    <div class="menu-item" onclick="showPane('heatmap')"><span class="menu-icon">🔥</span><span>Heatmap</span></div>
    <div class="menu-item" onclick="window.location.href='/currencies'"><span class="menu-icon">💱</span><span>Currencies</span></div>
    <div class="menu-item" onclick="showPane('history')"><span class="menu-icon">📜</span><span>History</span></div>
    {% if is_admin %}<div class="menu-item" onclick="window.location.href='/admin'"><span class="menu-icon">👑</span><span>Admin</span></div>{% endif %}
    <div class="menu-item" onclick="window.location.href='/profile'"><span class="menu-icon">👤</span><span>Profile</span></div>
    <div class="menu-item" onclick="logout()"><span class="menu-icon">🚪</span><span>Logout</span></div>
</div>

<div class="main-content" id="mainContent">
    <div class="navbar">
        <div class="navbar-title">Welcome, {{ username }}! {% if is_admin %}<span style="background:#ffb800;padding:4px 12px;border-radius:20px;font-size:12px;color:#000">👑 ADMIN</span>{% endif %}</div>
        <div><span id="lastUpdateTime" style="font-size:12px;color:#8892b0"></span></div>
    </div>

    <div id="analysisPane" class="content-pane active">
        <div style="margin-bottom:20px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
            <button onclick="loadAnalysis()">🔥 RUN ANALYSIS</button>
            <button onclick="refreshAnalysis()" class="secondary">🔄 Force Refresh</button>
            <input type="text" id="searchInput" class="search-box" placeholder="🔍 Search pair...">
        </div>
        <div id="loading" style="display:none"><div class="loading-skeleton" style="height:200px;border-radius:12px"></div></div>
        <div id="results"></div>
    </div>

    <div id="heatmapPane" class="content-pane">
        <h2 style="color:#00e5ff;margin-bottom:15px">Currency Strength & Economic Gauge</h2>
        <div id="heatmapContent"></div>
    </div>

    <div id="historyPane" class="content-pane">
        <button onclick="loadHistory()" class="secondary">Load History</button>
        <div id="historyContent" style="margin-top:20px"></div>
    </div>
</div>

<!-- Pair Detail Modal -->
<div class="modal" id="detailModal">
    <div class="modal-content">
        <h3 style="color:#00e5ff;margin-bottom:20px" id="modalPairTitle">EUR/USD Details</h3>
        <div id="modalDetailContent">Loading...</div>
        <div style="margin-top:20px;text-align:right">
            <button onclick="closeDetailModal()" class="secondary">Close</button>
        </div>
    </div>
</div>

<script>
    let currentData = null, heatmapData = null, favorites = {{ favorites|safe }};
    let currentResults = [];

    function toggleSidebar() {
        const sidebar = document.getElementById('sidebar');
        const main = document.getElementById('mainContent');
        sidebar.classList.toggle('collapsed');
        if (sidebar.classList.contains('collapsed')) {
            main.style.marginLeft = '80px';
        } else {
            main.style.marginLeft = '280px';
        }
    }

    function showPane(pane) {
        document.querySelectorAll('.content-pane').forEach(p => p.classList.remove('active'));
        document.getElementById(pane + 'Pane').classList.add('active');
        document.querySelectorAll('.menu-item').forEach(item => {
            item.classList.remove('active');
            const onclick = item.getAttribute('onclick');
            if (onclick && onclick.includes(`'${pane}'`)) {
                item.classList.add('active');
            }
        });
        if (pane === 'history') loadHistory();
    }

    async function loadAnalysis() {
        document.getElementById('loading').style.display = 'block';
        try {
            const res = await fetch('/api/analyze', {method:'POST'});
            if (!res.ok) throw new Error((await res.json()).error);
            const data = await res.json();
            currentData = data.results;
            heatmapData = data.heatmap;
            currentResults = data.results;
            displayResults(data.results);
            displayHeatmap(data.heatmap);
            document.getElementById('lastUpdateTime').innerHTML = 'Updated: ' + new Date().toLocaleTimeString();
        } catch(e) {
            document.getElementById('results').innerHTML = '<div style="color:#ff4d6d;padding:20px">❌ ' + e.message + '</div>';
        } finally {
            document.getElementById('loading').style.display = 'none';
        }
    }

    async function refreshAnalysis() {
        await fetch('/admin/refresh', {method:'POST'});
        loadAnalysis();
    }

    function filterResults() {
        const term = document.getElementById('searchInput').value.toLowerCase();
        const filtered = currentResults.filter(r => r.pair.toLowerCase().includes(term));
        displayResults(filtered);
    }

    function displayResults(data) {
        let html = '<div style="overflow-x:auto"><table><thead><tr><th>Pair</th><th>COT</th><th>Momentum</th><th>Economic</th><th>Trend</th><th>Seasonality</th><th>Overall</th></tr></thead><tbody>';
        data.forEach(item => {
            const isFav = favorites.includes(item.pair);
            html += `<tr>
                <td style="font-weight:bold">
                    <span class="clickable-pair" onclick="showPairDetail('${item.pair}')">${item.pair}</span>
                    <span class="favorite-star ${isFav?'active':''}" onclick="toggleFavorite('${item.pair}',this);event.stopPropagation();">⭐</span>
                </td>
                <td style="color:${item.cot.bias==='Bullish'?'#00e5a0':(item.cot.bias==='Bearish'?'#ff4d6d':'#ffb800')}">${item.cot.symbol} ${item.cot.bias}<br><small>[${item.cot.score}]</small></td>
                <td style="color:${item.cot.momentum==='Bullish'?'#00e5a0':(item.cot.momentum==='Bearish'?'#ff4d6d':'#ffb800')}">${item.cot.mom_symbol} ${item.cot.momentum}<br><small>[${item.cot.momentum_score}]</small></td>
                <td style="color:${item.economic.bias.includes('BULLISH')?'#00e5a0':(item.economic.bias.includes('BEARISH')?'#ff4d6d':'#ffb800')}">${item.economic.symbol}<br><small>[${item.economic.score}]</small></td>
                <td style="color:${item.trend.bias==='Bullish'?'#00e5a0':(item.trend.bias==='Bearish'?'#ff4d6d':'#ffb800')}">${item.trend.symbol}<br><small>[${item.trend.score}]</small></td>
                <td>${item.seasonality.bias}<br><small>[${item.seasonality.score}]</small></td>
                <td style="color:${item.overall.color};font-weight:bold">${item.overall.symbol} ${item.overall.bias}<br><strong>[${item.overall.score}]</strong></td>
            </tr>`;
        });
        html += '</tbody></table></div>';
        document.getElementById('results').innerHTML = html;
        document.getElementById('searchInput').addEventListener('input', filterResults);
    }

    function displayHeatmap(heatmap) {
        if (!heatmap) return;
        let html = '<div class="currency-grid">';
        heatmap.ranking.forEach(curr => {
            let currency = curr[0], score = curr[1], econPct = heatmap.econ_data[currency] || 50;
            let scoreColor = score>3?'#00e5a0':(score>1.5?'#66ffb3':(score>0.5?'#99ffcc':(score>-0.5?'#ffb800':(score>-1.5?'#ffb3c1':(score>-3?'#ff8099':'#ff4d6d')))));
            let circumference = 2 * Math.PI * 50;
            let dashOffset = circumference * (1 - econPct/100);
            let gaugeColor = econPct>=70?'#00e5a0':(econPct>=55?'#66ffb3':(econPct>=45?'#ffb800':(econPct>=30?'#ff8099':'#ff4d6d')));
            html += `<div class="currency-card" style="border-top:3px solid ${scoreColor}">
                <strong style="color:${scoreColor}">${currency}</strong>
                <div class="gauge-wrapper">
                    <svg class="gauge-svg" viewBox="0 0 120 120">
                        <circle class="gauge-bg-circle" cx="60" cy="60" r="50"></circle>
                        <circle class="gauge-fill-circle" cx="60" cy="60" r="50" style="stroke:${gaugeColor};stroke-dasharray:${circumference};stroke-dashoffset:${dashOffset}"></circle>
                    </svg>
                    <div class="gauge-center"><div class="gauge-value">${econPct.toFixed(1)}%</div></div>
                </div>
                <div>Score: ${score.toFixed(2)}</div>
            </div>`;
        });
        html += '</div>';
        document.getElementById('heatmapContent').innerHTML = html;
    }

    function toggleFavorite(pair, el) {
        const idx = favorites.indexOf(pair);
        if (idx > -1) favorites.splice(idx, 1);
        else favorites.push(pair);
        fetch('/api/favorites', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({favorites})});
        el.classList.toggle('active');
    }

    async function showPairDetail(pair) {
        document.getElementById('modalPairTitle').innerText = pair + ' Details';
        document.getElementById('modalDetailContent').innerHTML = '<div class="loading-skeleton" style="height:150px"></div>';
        document.getElementById('detailModal').style.display = 'flex';
        try {
            const res = await fetch('/api/pair_detail/' + encodeURIComponent(pair));
            const data = await res.json();
            let html = `
                <div style="background:#1a1f2e;padding:15px;border-radius:8px;margin-bottom:15px">
                    <h4 style="color:#00e5ff">${data.base.currency}</h4>
                    <div class="detail-row"><span>COT Net Position:</span> <span style="color:${data.base.cot_net>=0?'#00e5a0':'#ff4d6d'}">${data.base.cot_net.toLocaleString()}</span></div>
                    <div class="detail-row"><span>COT Weekly Change:</span> <span style="color:${data.base.cot_change>=0?'#00e5a0':'#ff4d6d'}">${data.base.cot_change.toLocaleString()}</span></div>
                    <div class="detail-row"><span>Economic Sentiment:</span> <span>${data.base.econ_pct.toFixed(1)}%</span></div>
                </div>
                <div style="background:#1a1f2e;padding:15px;border-radius:8px">
                    <h4 style="color:#00e5ff">${data.quote.currency}</h4>
                    <div class="detail-row"><span>COT Net Position:</span> <span style="color:${data.quote.cot_net>=0?'#00e5a0':'#ff4d6d'}">${data.quote.cot_net.toLocaleString()}</span></div>
                    <div class="detail-row"><span>COT Weekly Change:</span> <span style="color:${data.quote.cot_change>=0?'#00e5a0':'#ff4d6d'}">${data.quote.cot_change.toLocaleString()}</span></div>
                    <div class="detail-row"><span>Economic Sentiment:</span> <span>${data.quote.econ_pct.toFixed(1)}%</span></div>
                </div>
            `;
            document.getElementById('modalDetailContent').innerHTML = html;
        } catch(e) {
            document.getElementById('modalDetailContent').innerHTML = '<p style="color:#ff4d6d">Error loading details</p>';
        }
    }

    function closeDetailModal() {
        document.getElementById('detailModal').style.display = 'none';
    }

    async function loadHistory() {
        const r = await fetch('/api/history');
        const d = await r.json();
        let html = '<table><thead><tr><th>Date</th><th>Pair</th><th>COT</th><th>Economic</th><th>Trend</th><th>Seasonality</th><th>Overall</th></tr></thead><tbody>';
        d.forEach(i => {
            html += `<tr><td>${i.date}</td><td>${i.pair}</td><td>${i.cot_bias}</td><td>${i.econ_bias}</td><td>${i.trend_score}</td><td>${i.seasonality_bias||'N/A'}</td><td>${i.overall_bias}</td></tr>`;
        });
        html += '</tbody></table>';
        document.getElementById('historyContent').innerHTML = html;
    }

    function logout() { fetch('/logout').then(() => window.location.href = '/login'); }

    loadAnalysis();
</script>
</body>
</html>''')

    # Admin template (with activation buttons)
    with open('templates/admin.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html>
<head><title>Admin - Tradion</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',sans-serif;background:#0B0F1A;color:#fff}
.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed}
.logo{font-size:22px;color:#00e5ff;margin-bottom:30px}
.menu-item{padding:12px 15px;margin:5px 0;border-radius:8px;cursor:pointer;color:#a0b0c0}
.menu-item:hover{background:rgba(0,229,255,0.1);color:#00e5ff}
.main-content{margin-left:280px;padding:20px}
.card{background:#121826;border-radius:12px;padding:20px;margin-bottom:20px;border:1px solid #2a3040}
button{padding:8px 16px;background:#00e5ff;color:#0B0F1A;border:none;border-radius:6px;cursor:pointer;font-weight:bold}
button.secondary{background:transparent;border:1px solid #00e5ff;color:#00e5ff}
.content-pane{display:none}
.content-pane.active{display:block}
table{width:100%;border-collapse:collapse}
th,td{padding:8px;border-bottom:1px solid #2a3040}
input,select{padding:6px;background:#1a1f2e;border:1px solid #2a3040;color:#fff;border-radius:4px;width:100%;margin-bottom:10px}
.modal{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.7);z-index:1000;justify-content:center;align-items:center}
.modal-content{background:#121826;padding:25px;border-radius:16px;width:90%;max-width:500px;border:1px solid #2a3040}
.status-badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:12px}
.status-active{background:#00e5a0;color:#000}
.status-inactive{background:#ff4d6d;color:#fff}
</style>
</head>
<body>
<div class="sidebar">
    <div class="logo">👑 ADMIN PANEL</div>
    <div class="menu-item" onclick="showPane('cot')">📁 COT Upload</div>
    <div class="menu-item" onclick="showPane('econ')">📊 Econ Upload</div>
    <div class="menu-item" onclick="showPane('seasonality')">📅 Seasonality</div>
    <div class="menu-item" onclick="showPane('users')">👥 Users</div>
    <div class="menu-item" onclick="window.location.href='/dashboard'">⬅ Back to Dashboard</div>
</div>
<div class="main-content">
    <div id="cotPane" class="content-pane active">
        <div class="card">
            <h2>Upload COT Data</h2>
            <input type="file" id="cotFile" accept=".xlsx">
            <button onclick="uploadCOT()">Upload</button>
            <div id="cotStatus" style="margin-top:10px"></div>
        </div>
    </div>
    <div id="econPane" class="content-pane">
        <div class="card">
            <h2>Upload Economic Data</h2>
            <input type="file" id="econFile" accept=".xlsx">
            <button onclick="uploadEcon()">Upload</button>
            <div id="econStatus" style="margin-top:10px"></div>
        </div>
    </div>
    <div id="seasonalityPane" class="content-pane">
        <div class="card">
            <h2>Seasonality Configurations</h2>
            <button onclick="openSeasonModal()">+ Add Seasonality Pattern</button>
            <div id="seasonList" style="margin-top:20px"></div>
        </div>
    </div>
    <div id="usersPane" class="content-pane">
        <div class="card">
            <h2>User Management</h2>
            <button onclick="loadUsers()">Refresh</button>
            <div id="usersList" style="margin-top:15px"></div>
        </div>
    </div>
</div>

<!-- Seasonality Modal -->
<div class="modal" id="seasonModal">
    <div class="modal-content">
        <h3 id="modalTitle" style="color:#00e5ff;margin-bottom:20px">Add Seasonality</h3>
        <form id="seasonForm">
            <input type="hidden" id="seasonId">
            <label>Pair</label>
            <select id="seasonPair" required></select>
            <label>Description (e.g. "Bullish (70%)")</label>
            <input type="text" id="seasonDesc" placeholder="Bullish (70%)" required>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                <div><label>Start Month</label><input type="number" id="startMonth" min="1" max="12" required></div>
                <div><label>Start Day</label><input type="number" id="startDay" min="1" max="31" required></div>
                <div><label>End Month</label><input type="number" id="endMonth" min="1" max="12" required></div>
                <div><label>End Day</label><input type="number" id="endDay" min="1" max="31" required></div>
            </div>
            <label>Score (positive=bullish, negative=bearish)</label>
            <input type="number" step="0.1" id="seasonScore" placeholder="e.g. 1.5 or -1" required>
            <div style="display:flex;gap:10px;margin-top:20px">
                <button type="submit">Save</button>
                <button type="button" onclick="closeSeasonModal()" class="secondary">Cancel</button>
            </div>
        </form>
    </div>
</div>

<script>
    const allPairs = {{ ALL_PAIRS|tojson }};
    let seasonData = [];

    function showPane(pane){
        document.querySelectorAll('.content-pane').forEach(p=>p.classList.remove('active'));
        document.getElementById(pane+'Pane').classList.add('active');
        if(pane==='users') loadUsers();
        if(pane==='seasonality') loadSeasonality();
    }

    async function uploadCOT(){
        const file=document.getElementById('cotFile').files[0];
        if(!file) return;
        const fd=new FormData(); fd.append('file',file);
        const res=await fetch('/admin/cot/upload',{method:'POST',body:fd});
        const data=await res.json();
        document.getElementById('cotStatus').innerHTML=data.success?'✅ Uploaded successfully':'❌ Error uploading';
    }

    async function uploadEcon(){
        const file=document.getElementById('econFile').files[0];
        if(!file) return;
        const fd=new FormData(); fd.append('file',file);
        const res=await fetch('/admin/econ/upload',{method:'POST',body:fd});
        const data=await res.json();
        document.getElementById('econStatus').innerHTML=data.success?'✅ Uploaded successfully':'❌ Error uploading';
    }

    async function loadUsers(){
        const res=await fetch('/admin/users');
        const data=await res.json();
        let html='<table><tr><th>User</th><th>Email</th><th>Admin</th><th>Status</th><th>Actions</th></tr>';
        data.users.forEach(u=>{
            const statusClass = u.is_active ? 'status-active' : 'status-inactive';
            const statusText = u.is_active ? 'Active' : 'Inactive';
            const actionBtn = u.is_active ?
                `<button onclick="deactivateUser(${u.id})" class="secondary">Deactivate</button>` :
                `<button onclick="activateUser(${u.id})">Activate</button>`;
            html+=`<tr>
                <td>${u.username}</td><td>${u.email}</td><td>${u.is_admin?'Yes':'No'}</td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                <td>${actionBtn}</td>
            </tr>`;
        });
        html+='</table>';
        document.getElementById('usersList').innerHTML=html;
    }

    async function activateUser(id) {
        await fetch(`/admin/users/${id}/activate`, {method:'POST'});
        loadUsers();
    }

    async function deactivateUser(id) {
        await fetch(`/admin/users/${id}/deactivate`, {method:'POST'});
        loadUsers();
    }

    async function loadSeasonality(){
        const res = await fetch('/admin/seasonality');
        seasonData = await res.json();
        let html = '<table><tr><th>Pair</th><th>Description</th><th>Start</th><th>End</th><th>Score</th><th>Actions</th></tr>';
        seasonData.forEach(c => {
            html += `<tr><td>${c.pair}</td><td>${c.description}</td><td>${c.start_month}/${c.start_day}</td><td>${c.end_month}/${c.end_day}</td><td>${c.score}</td>
                <td><button onclick="editSeason(${c.id})">Edit</button> <button onclick="deleteSeason(${c.id})" class="secondary">Del</button></td></tr>`;
        });
        html += '</table>';
        document.getElementById('seasonList').innerHTML = html;
        const select = document.getElementById('seasonPair');
        select.innerHTML = '';
        allPairs.forEach(p => { 
            const pairStr = p[0]+'/'+p[1];
            const opt = document.createElement('option'); 
            opt.value = pairStr; 
            opt.text = pairStr; 
            select.appendChild(opt); 
        });
    }

    function openSeasonModal(){ 
        document.getElementById('modalTitle').innerText='Add Seasonality'; 
        document.getElementById('seasonForm').reset(); 
        document.getElementById('seasonId').value=''; 
        document.getElementById('seasonModal').style.display='flex'; 
    }

    function closeSeasonModal(){ 
        document.getElementById('seasonModal').style.display='none'; 
    }

    function editSeason(id){
        const c = seasonData.find(s => s.id === id);
        if(!c) return;
        document.getElementById('modalTitle').innerText='Edit Seasonality';
        document.getElementById('seasonId').value = c.id;
        document.getElementById('seasonPair').value = c.pair;
        document.getElementById('seasonDesc').value = c.description;
        document.getElementById('startMonth').value = c.start_month;
        document.getElementById('startDay').value = c.start_day;
        document.getElementById('endMonth').value = c.end_month;
        document.getElementById('endDay').value = c.end_day;
        document.getElementById('seasonScore').value = c.score;
        document.getElementById('seasonModal').style.display='flex';
    }

    async function deleteSeason(id){ 
        if(confirm('Delete this seasonality pattern?')){ 
            await fetch('/admin/seasonality/'+id,{method:'DELETE'}); 
            loadSeasonality(); 
        } 
    }

    document.getElementById('seasonForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const id = document.getElementById('seasonId').value;
        const data = {
            pair: document.getElementById('seasonPair').value,
            description: document.getElementById('seasonDesc').value,
            start_month: parseInt(document.getElementById('startMonth').value),
            start_day: parseInt(document.getElementById('startDay').value),
            end_month: parseInt(document.getElementById('endMonth').value),
            end_day: parseInt(document.getElementById('endDay').value),
            score: parseFloat(document.getElementById('seasonScore').value)
        };
        const url = id ? '/admin/seasonality/'+id : '/admin/seasonality';
        const method = id ? 'PUT' : 'POST';
        await fetch(url, {method, headers:{'Content-Type':'application/json'}, body:JSON.stringify(data)});
        closeSeasonModal();
        loadSeasonality();
    });

    loadUsers();
    loadSeasonality();
</script>
</body>
</html>''')

    # Profile template (fixed overlap)
    with open('templates/profile.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html>
<head><title>Profile - Tradion</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',sans-serif;background:#0B0F1A;color:#fff;display:flex}
.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed}
.logo{font-size:24px;color:#00e5ff;margin-bottom:30px}
.menu-item{padding:12px 15px;margin:5px 0;border-radius:8px;cursor:pointer;color:#a0b0c0}
.menu-item:hover{background:rgba(0,229,255,0.1);color:#00e5ff}
.main-content{flex:1;margin-left:280px;padding:30px}
.container{max-width:600px;margin:50px auto;padding:30px;background:#121826;border-radius:20px;border:1px solid #2a3040}
.container h2{color:#00e5ff;margin-bottom:20px}
.container div{margin-bottom:20px;font-size:1.1em}
.btn{display:inline-block;padding:10px 20px;background:#00e5ff;color:#000;text-decoration:none;border-radius:8px;font-weight:bold;margin-top:10px}
</style>
</head>
<body>
<div class="sidebar">
    <div class="logo">⚡ Tradion</div>
    <div class="menu-item" onclick="window.location.href='/dashboard'">📊 Dashboard</div>
    <div class="menu-item" onclick="logout()">🚪 Logout</div>
</div>
<div class="main-content">
    <div class="container">
        <h2>Profile</h2>
        <div>👤 Username: <strong>{{ username }}</strong></div>
        <a href="/dashboard" class="btn">← Back to Dashboard</a>
    </div>
</div>
<script>
function logout(){fetch('/logout').then(()=>window.location.href='/login');}
</script>
</body>
</html>''')

    # Currencies template (unchanged)
    with open('templates/currencies.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html>
<head>
    <title>Currencies - Tradion</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;display:flex}
        .sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100}
        .sidebar .logo{font-size:24px;font-weight:800;color:#00e5ff;text-align:center;margin-bottom:30px;padding-bottom:20px;border-bottom:1px solid #2a3040}
        .sidebar .menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:10px;cursor:pointer;transition:all 0.2s;color:#a0b0c0}
        .sidebar .menu-item:hover{background:rgba(0,229,255,0.08);color:#00e5ff}
        .sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;font-weight:bold}
        .sidebar .menu-icon{font-size:20px;margin-right:12px}
        .main-content{flex:1;margin-left:280px;padding:20px 30px}
        .navbar{background:#121826;padding:15px 25px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #2a3040;border-radius:0 0 16px 16px;margin-bottom:20px}
        .navbar-title{font-size:18px;color:#00e5ff}
        button{padding:10px 20px;background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;border:none;border-radius:8px;cursor:pointer;font-weight:bold}
        button.secondary{background:transparent;border:1px solid #00e5ff;color:#00e5ff}
        table{width:100%;border-collapse:collapse;margin-top:20px;background:#121826;font-size:14px;border-radius:12px;overflow:hidden}
        th,td{padding:14px 12px;text-align:center;border-bottom:1px solid #2a3040}
        th{background:rgba(0,229,255,0.1);color:#00e5ff;font-weight:600}
        .gauge-bar{width:100px;height:8px;background:#2a3040;border-radius:4px;overflow:hidden;margin:0 auto}
        .gauge-fill{height:100%;border-radius:4px;transition:width 0.3s}
        .positive{color:#00e5a0}
        .negative{color:#ff4d6d}
        .neutral{color:#ffb800}
        .loading-skeleton{background:linear-gradient(90deg,#1a1f2e,#2a3040,#1a1f2e);background-size:200% 100%;animation:shimmer 1.5s infinite}
        @keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
        .last-updated{font-size:12px;color:#8892b0;margin-top:10px}
    </style>
</head>
<body>
<div class="sidebar">
    <div class="logo">⚡ Tradion</div>
    <div class="menu-item" onclick="window.location.href='/dashboard'"><div class="menu-icon">📊</div>Dashboard</div>
    <div class="menu-item active" onclick="window.location.href='/currencies'"><div class="menu-icon">💱</div>Currencies</div>
    {% if is_admin %}<div class="menu-item" onclick="window.location.href='/admin'"><div class="menu-icon">👑</div>Admin</div>{% endif %}
    <div class="menu-item" onclick="window.location.href='/profile'"><div class="menu-icon">👤</div>Profile</div>
    <div class="menu-item" onclick="logout()"><div class="menu-icon">🚪</div>Logout</div>
</div>

<div class="main-content">
    <div class="navbar">
        <div class="navbar-title">Currencies · Economic & COT Data</div>
        <div><button onclick="loadCurrencies()" class="secondary">🔄 Refresh</button></div>
    </div>

    <div id="loading" style="display:none"><div class="loading-skeleton" style="height:200px;border-radius:12px"></div></div>
    <div id="currencyTable"></div>
    <div id="lastUpdated" class="last-updated"></div>
</div>

<script>
    async function loadCurrencies() {
        document.getElementById('loading').style.display = 'block';
        document.getElementById('currencyTable').innerHTML = '';
        try {
            const res = await fetch('/api/currencies');
            if (!res.ok) throw new Error('Failed to load');
            const data = await res.json();
            displayCurrencies(data.currencies);
            let updateText = '';
            if (data.cot_last_updated) updateText += `COT updated: ${new Date(data.cot_last_updated).toLocaleString()} · `;
            if (data.econ_last_updated) updateText += `Economic updated: ${new Date(data.econ_last_updated).toLocaleString()}`;
            document.getElementById('lastUpdated').innerHTML = updateText || 'File timestamps unavailable';
        } catch(e) {
            document.getElementById('currencyTable').innerHTML = '<div style="color:#ff4d6d;padding:20px">❌ Error loading currency data</div>';
        } finally {
            document.getElementById('loading').style.display = 'none';
        }
    }

    function displayCurrencies(currencies) {
        let html = '<table><thead><tr><th>Currency</th><th>Economic Sentiment</th><th>COT Net Position</th><th>COT Weekly Change</th></tr></thead><tbody>';
        currencies.forEach(c => {
            const econColor = c.econ_pct >= 55 ? '#00e5a0' : (c.econ_pct <= 45 ? '#ff4d6d' : '#ffb800');
            const netClass = c.cot_net >= 0 ? 'positive' : 'negative';
            const changeClass = c.cot_change >= 0 ? 'positive' : 'negative';
            html += `<tr>
                <td style="font-weight:bold;font-size:1.1em">${c.currency}</td>
                <td>
                    <div style="display:flex;align-items:center;justify-content:center;gap:8px">
                        <span style="color:${econColor};font-weight:bold">${c.econ_pct.toFixed(1)}%</span>
                        <div class="gauge-bar"><div class="gauge-fill" style="width:${c.econ_pct}%;background:${econColor}"></div></div>
                    </div>
                </td>
                <td class="${netClass}">${c.cot_net.toLocaleString()}</td>
                <td class="${changeClass}">${c.cot_change.toLocaleString()}</td>
            </tr>`;
        });
        html += '</tbody></table>';
        document.getElementById('currencyTable').innerHTML = html;
    }

    function logout() { fetch('/logout').then(() => window.location.href = '/login'); }

    loadCurrencies();
</script>
</body>
</html>''')

# -----------------------------
# MAIN
# -----------------------------
if __name__ == '__main__':
    migrate_database()
    # Tables are already created at module level; no need to recreate here.

    create_templates()

    def open_browser():
        time.sleep(2)
        webbrowser.open('http://localhost:5000')

    threading.Thread(target=open_browser, daemon=True).start()

    print("=" * 50)
    print("Tradion - Dark Pro Trader Edition")
    print("Admin: Karlmax / admin4125")
    print("=" * 50)

    app.run(debug=False, use_reloader=False, host='0.0.0.0', port=5000)