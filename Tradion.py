from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import os
from fredapi import Fred
import requests
import time
import pandas as pd
import numpy as np
import json
import traceback
import threading
import webbrowser
from functools import wraps
import yfinance as yf
from pathlib import Path
import sqlite3
import hashlib

app = Flask(__name__)
# -------- PASTE THE DATABASE CONFIGURATION HERE --------
import os

database_url = os.environ.get('DATABASE_URL')
if database_url:
    # Only replace the protocol; do NOT add ?sslmode=require
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://', 1)
    app.config['SQLALCHEMY_DATABASE_URI'] = database_url
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_pre_ping': True,
        'pool_recycle': 300,
        'pool_size': 5,
        'max_overflow': 10
    }
else:
    basedir = os.path.abspath(os.path.dirname(__file__))
    app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{os.path.join(basedir, "users.db")}'
# --------------------------------------------------------
app.secret_key = os.environ.get('SECRET_KEY', 'dev-fallback-key-for-local-only')
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

class EconomicIndicator(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    currency = db.Column(db.String(5), nullable=False)
    indicator_name = db.Column(db.String(200), nullable=False)
    forecast = db.Column(db.Float, default=0)
    actual = db.Column(db.Float, default=0)
    is_lower_better = db.Column(db.Boolean, default=False)
    category = db.Column(db.String(50), default='General')
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class COTData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    currency = db.Column(db.String(5), nullable=False, unique=True)
    net_position = db.Column(db.Float, default=0)          # auto-computed = longs - shorts
    weekly_change = db.Column(db.Float, default=0)
    longs = db.Column(db.Float, default=0)                 # non-commercial longs
    shorts = db.Column(db.Float, default=0)                # non-commercial shorts
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class SentimentData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pair = db.Column(db.String(20), nullable=False, unique=True)
    long_pct = db.Column(db.Float, default=50.0)
    short_pct = db.Column(db.Float, default=50.0)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class MonthlyBias(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pair = db.Column(db.String(20), nullable=False)
    month = db.Column(db.Integer, nullable=False)  # 1-12
    bias = db.Column(db.String(20), nullable=False, default='Neutral')  # Bullish/Bearish/Neutral
    __table_args__ = (db.UniqueConstraint('pair', 'month', name='unique_pair_month'),)

class SeasonalityDateRange(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pair = db.Column(db.String(20), nullable=False)
    start_month = db.Column(db.Integer, nullable=False)
    start_day = db.Column(db.Integer, nullable=False)
    end_month = db.Column(db.Integer, nullable=False)
    end_day = db.Column(db.Integer, nullable=False)
    bias = db.Column(db.String(20), nullable=False, default='Bullish')  # Bullish/Bearish

class COTHistory(db.Model):
    __tablename__ = 'cot_history'
    id = db.Column(db.Integer, primary_key=True)
    currency = db.Column(db.String(5), nullable=False, index=True)
    report_date = db.Column(db.Date, nullable=False)
    long_positions = db.Column(db.Float, default=0)
    short_positions = db.Column(db.Float, default=0)
    net_positions = db.Column(db.Float, default=0)
    weekly_change = db.Column(db.Float, default=0)
    bias = db.Column(db.Integer, default=0)
    change_longs = db.Column(db.Float, default=0)   # NEW
    change_shorts = db.Column(db.Float, default=0)  # NEW
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (db.UniqueConstraint('currency', 'report_date', name='unique_currency_date'),)
    
class RetailSentimentHistory(db.Model):
    __tablename__ = 'retail_sentiment_history'
    id = db.Column(db.Integer, primary_key=True)
    pair = db.Column(db.String(20), nullable=False, index=True)
    long_percentage = db.Column(db.Float, default=50.0)
    short_percentage = db.Column(db.Float, default=50.0)
    long_positions = db.Column(db.Integer, default=0)
    short_positions = db.Column(db.Integer, default=0)
    retail_bias = db.Column(db.Integer, default=0)  # 1=bullish, 0=neutral, -1=bearish (contrarian)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (db.Index('idx_pair_timestamp', 'pair', 'timestamp'),)
    
    
class AssetScoreHistory(db.Model):
    __tablename__ = 'asset_score_history'
    id = db.Column(db.Integer, primary_key=True)
    asset = db.Column(db.String(10), nullable=False, index=True)  # USD, EUR, GBP, etc.
    score = db.Column(db.Float, nullable=False)
    recorded_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    __table_args__ = (db.Index('idx_asset_recorded', 'asset', 'recorded_at'),)   

# -----------------------------
# DATABASE INITIALIZATION & MIGRATIONS
# -----------------------------
with app.app_context():
    db.create_all()
    
    

    # Add category column to economic_indicator if missing (safe for both SQLite and PostgreSQL)
    try:
        db.session.execute(db.text(
            "ALTER TABLE economic_indicator ADD COLUMN category VARCHAR(50) DEFAULT 'General'"
        ))
        db.session.commit()
    except Exception:
        db.session.rollback()

    # Add change_longs and change_shorts to cot_history if missing
    try:
        db.session.execute(db.text("ALTER TABLE cot_history ADD COLUMN change_longs FLOAT DEFAULT 0"))
        db.session.commit()
    except Exception:
        db.session.rollback()
    try:
        db.session.execute(db.text("ALTER TABLE cot_history ADD COLUMN change_shorts FLOAT DEFAULT 0"))
        db.session.commit()
    except Exception:
        db.session.rollback()

    # Create default admin if missing
    admin = User.query.filter_by(username='Karlmax').first()
    if not admin:
        admin = User(username='Karlmax', email='maxwellkarani89@gmail.com',
                     is_admin=True, is_active=True)
        admin.set_password('admin4125')
        db.session.add(admin)
        db.session.commit()
        
        
  
    # List of all currencies (same as MAJOR_CURRENCIES defined later)
    all_currencies = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", "XAU", "BTC"]

    for curr in all_currencies:
        # Central Bank Guidance (Monetary Policy) – for all currencies
        if not EconomicIndicator.query.filter_by(currency=curr, indicator_name="Central Bank Guidance").first():
            db.session.add(EconomicIndicator(
                currency=curr,
                indicator_name="Central Bank Guidance",
                forecast=0.0,
                actual=0.0,
                is_lower_better=False,
                category="Monetary Policy"
            ))

    # Average Hourly Earnings (Labor Market) – only for USD
    if not EconomicIndicator.query.filter_by(currency="USD", indicator_name="Average Hourly Earnings").first():
        db.session.add(EconomicIndicator(
            currency="USD",
            indicator_name="Average Hourly Earnings",
            forecast=0.0,
            actual=0.0,
            is_lower_better=False,
            category="Labor Market"
        ))

    db.session.commit()
        
        
        
    

# -----------------------------
# CONFIGURATION
# -----------------------------
UPLOAD_FOLDER = Path.cwd() / 'uploads'
UPLOAD_FOLDER.mkdir(exist_ok=True)

COT_FILE_PATH = str(UPLOAD_FOLDER / "Max_3.xlsx")
ECON_DATA_FILE_PATH = str(UPLOAD_FOLDER / "Economical_analyzer.xlsx")

MAJOR_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD","XAU","BTC"]

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

# Add XAUUSD as a special pair
ALL_PAIRS.append(("XAU", "USD"))
ALL_PAIRS.append(("BTC", "USD"))

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
    "XAU/USD": "GC=F",
    "BTC/USD": "BTC-USD",
}

LOWER_BETTER_INDICATORS = ["Unemployment Rate", "Jobless Claims", "Claims", "Unemployment Claims"]

cached_analysis = None
cached_heatmap = None
last_analysis_time = None
manual_refresh_triggered = False
cached_bond_score = None
cached_bond_score_time = None
BOND_CACHE_SECONDS = 3600  # 1 hour
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

@app.route('/debug/save_all_now')
@login_required
@admin_required
def debug_save_all_now():
    with app.app_context():
        # Save currencies
        for currency in MAJOR_CURRENCIES:
            try:
                score = get_asset_score(currency)
                save_asset_score_history(currency, score)
                print(f"Saved currency {currency}: {score}")
            except Exception as e:
                print(f"Error {currency}: {e}")
        # Save forex pairs
        for base, quote in ALL_PAIRS:
            pair = f"{base}/{quote}"
            try:
                weighted = calculate_weighted_pair_score(pair, base, quote)
                total_score = weighted["total_raw"]
                save_asset_score_history(pair, total_score)
                print(f"Saved pair {pair}: {total_score}")
            except Exception as e:
                print(f"Error {pair}: {e}")
    return jsonify({'success': True, 'message': 'All scores saved (currencies + pairs)'})

# -----------------------------
# DEBUG endpoint to check economic surprise calculation
# -----------------------------
@app.route('/debug/econ/<currency>')
@login_required
def debug_econ(currency):
    currency = currency.upper()
    indicators = EconomicIndicator.query.filter_by(currency=currency).all()
    result = {
        'currency': currency,
        'indicators': []
    }
    bullish = bearish = 0
    for ind in indicators:
        if ind.forecast == 0 and ind.actual == 0:
            continue
        if "Employment Change" in ind.indicator_name:
            continue
        
        lower_better = ind.is_lower_better
        
        if lower_better:
            is_bullish = (ind.actual < ind.forecast)
            is_bearish = (ind.actual > ind.forecast)
        else:
            is_bullish = (ind.actual > ind.forecast)
            is_bearish = (ind.actual < ind.forecast)
        
        if is_bullish:
            bullish += 1
        elif is_bearish:
            bearish += 1
        
        result['indicators'].append({
            'name': ind.indicator_name,
            'forecast': ind.forecast,
            'actual': ind.actual,
            'lower_better': lower_better,
            'is_bullish': is_bullish,
            'is_bearish': is_bearish
        })
    
    total = bullish + bearish
    percent = (bullish / total * 100) if total > 0 else 50
    result['bullish_count'] = bullish
    result['bearish_count'] = bearish
    result['percentage'] = round(percent, 1)
    
    return jsonify(result)
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
# TREND ANALYSIS (21-day SMA)
# -----------------------------
def get_trend_score_21d(pair_symbol):
    """Return 1 if price > 21-day SMA, -1 if below, 0 if equal or no data."""
    try:
        if not pair_symbol.endswith('=X'):
            pair_symbol = pair_symbol + '=X'
        ticker = yf.Ticker(pair_symbol)
        hist = ticker.history(period="2mo", interval="1d")
        if len(hist) < 21:
            return 0
        sma_21 = hist['Close'].rolling(window=21).mean().iloc[-1]
        current_price = hist['Close'].iloc[-1]
        if current_price > sma_21:
            return 1
        elif current_price < sma_21:
            return -1
        return 0
    except:
        return 0

# (Legacy 100-day trend kept for other uses if needed, but not used in scorecard)
def get_trend_score(pair_symbol):
    return get_trend_score_21d(pair_symbol)


def get_us_2yr_bond_trend_score() -> int:
    global cached_bond_score, cached_bond_score_time
    import time
    
    # Return cached value if still fresh
    now = time.time()
    if cached_bond_score is not None and cached_bond_score_time is not None:
        if (now - cached_bond_score_time) < BOND_CACHE_SECONDS:
            return cached_bond_score
    
    # Otherwise fetch new data
    api_key = '98adbefbd0ae0c2360298858644f3a19'
    url = f'https://api.stlouisfed.org/fred/series/observations?series_id=DGS2&api_key={api_key}&file_type=json&sort_order=desc&limit=100'
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"FRED API error: HTTP {response.status_code}")
            return cached_bond_score if cached_bond_score is not None else 0
        
        data = response.json()
        observations = data.get('observations', [])
        yields = []
        for obs in observations:
            val = obs.get('value')
            if val and val != '.':
                try:
                    yields.append(float(val))
                except:
                    pass
        
        if len(yields) < 21:
            print(f"Only {len(yields)} yields, need 21")
            return cached_bond_score if cached_bond_score is not None else 0
        
        yields.reverse()
        series = pd.Series(yields)
        sma_21 = series.rolling(window=21).mean().iloc[-1]
        current = series.iloc[-1]
        
        if current > sma_21:
            score = 1
        elif current < sma_21:
            score = -1
        else:
            score = 0
        
        # Store in cache
        cached_bond_score = score
        cached_bond_score_time = now
        return score
        
    except Exception as e:
        print(f"FRED bond error: {e}")
        return cached_bond_score if cached_bond_score is not None else 0


# -----------------------------
# SEASONALITY ANALYSIS
# -----------------------------
def get_seasonality_bias(pair):
    today = datetime.now()
    current_month = today.month

    month_bias = MonthlyBias.query.filter_by(pair=pair, month=current_month).first()
    if not month_bias or month_bias.bias == 'Neutral':
        return "Neutral", 0

    ranges = SeasonalityDateRange.query.filter_by(pair=pair).all()
    in_range = False
    for r in ranges:
        start = (r.start_month, r.start_day)
        end = (r.end_month, r.end_day)
        current = (current_month, today.day)
        if start <= end:
            if start <= current <= end:
                in_range = True
                break
        else:
            if current >= start or current <= end:
                in_range = True
                break

    if in_range:
        return month_bias.bias, 2
    else:
        return month_bias.bias, 1
    
# -----------------------------
# WEIGHTED MACRO SCORING ENGINE (CORRECTED)
# -----------------------------
WEIGHTS = {
    # Inflation (total max = 8)
    "CPI YoY (%)": 3,
    "PCE YoY (%)": 3,          # Core PCE YoY (%)
    "PPI YoY (%)": 2,
    # Economic Growth (total max = 10)
    "GDP Growth Rate QoQ (%)": 3,
    "Services PMI": 2,
    "Manufacturing PMI": 2,
    "Retail Sales MoM (%)": 2,
    "Consumer Confidence": 1,
    # Labor Market (total max = 11)
    "NFP (K)": 3,
    "Average Hourly Earnings": 3,
    "Unemployment Rate (%)": 2,
    "JOLTS Job Openings (M)": 1,
    "ADP (K)": 1,
    "Unemployment Claims (K)": 1,
    # Monetary Policy (total max = 3)
    "Interest Rate Decision": 3,
    # Flow / Positioning (total max = 3)
    "COT_ALIGNMENT": 2,
    "RETAIL_SENTIMENT": 1,
    # Technical (max = 2)
    "TREND_21D_SMA": 2,
    # Seasonality (max = 1)
    "SEASONALITY": 1,
}
# Total maximum raw score = 38, minimum = -38
MAX_RAW_SCORE = 20

def get_indicator_directional_score(currency: str, indicator_name: str,
                                    forecast: float, actual: float,
                                    is_lower_better: bool) -> int:
    """Return 1 (Bullish), 0 (Neutral), or -1 (Bearish)."""
    if indicator_name == "Interest Rate Decision":
        if actual > forecast:
            return 1
        elif actual < forecast:
            return -1
        return 0
    if forecast == 0 and actual == 0:
        return 0
    if is_lower_better:
        if actual < forecast:
            return 1
        elif actual > forecast:
            return -1
    else:
        if actual > forecast:
            return 1
        elif actual < forecast:
            return -1
    return 0

def get_currency_indicator_scores(currency: str) -> dict:
    """Return dict {indicator_name: directional_score} for a currency."""
    indicators = EconomicIndicator.query.filter_by(currency=currency.upper()).all()
    scores = {}
    for ind in indicators:
        if ind.forecast == 0 and ind.actual == 0:
            continue
        name = ind.indicator_name
        if "Employment Change" in name:
            continue
        score = get_indicator_directional_score(currency, name, ind.forecast,
                                                ind.actual, ind.is_lower_better)
        scores[name] = score
    return scores

def normalize_pair_score(base_score: int, quote_score: int) -> int:
    """Return -1, 0, or 1 based on base_score - quote_score."""
    diff = base_score - quote_score
    if diff > 0:
        return 1
    elif diff < 0:
        return -1
    return 0

def get_cot_alignment_score(base_curr: str, quote_curr: str) -> int:
    """
    Returns +2 if both net and momentum are bullish for the pair,
    -2 if both are bearish, otherwise 0.
    """
    base_cot = COTData.query.filter_by(currency=base_curr.upper()).first()
    quote_cot = COTData.query.filter_by(currency=quote_curr.upper()).first()

    base_net = base_cot.net_position if base_cot else 0
    base_mom = base_cot.weekly_change if base_cot else 0
    quote_net = quote_cot.net_position if quote_cot else 0
    quote_mom = quote_cot.weekly_change if quote_cot else 0

    base_net_dir = 1 if base_net > 0 else (-1 if base_net < 0 else 0)
    base_mom_dir = 1 if base_mom > 0 else (-1 if base_mom < 0 else 0)
    quote_net_dir = 1 if quote_net > 0 else (-1 if quote_net < 0 else 0)
    quote_mom_dir = 1 if quote_mom > 0 else (-1 if quote_mom < 0 else 0)

    # Pair net direction: base - quote
    pair_net = base_net_dir - quote_net_dir
    pair_mom = base_mom_dir - quote_mom_dir

    # Both must be positive for +2, both negative for -2
    if pair_net > 0 and pair_mom > 0:
        return 2
    elif pair_net < 0 and pair_mom < 0:
        return -2
    return 0

# -----------------------------
# RETAIL SENTIMENT (Myfxbook API)
# -----------------------------
MYFXBOOK_API_URL = "https://www.myfxbook.com/api/get-community-outlook.json?session=DSL07vu14QxHWErTIAFrH40"

def fetch_retail_sentiment_from_api():
    """Fetch raw data from Myfxbook API. Returns list of dicts or None."""
    try:
        import requests
        resp = requests.get(MYFXBOOK_API_URL, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status'):
                return data.get('symbols', [])
        return None
    except Exception as e:
        print(f"Error fetching retail sentiment: {e}")
        return None

def normalize_retail_bias(long_pct: float) -> int:
    """Contrarian bias: 1 = bullish (crowd short), -1 = bearish (crowd long), 0 = neutral."""
    if long_pct > 60:
        return -1
    elif long_pct < 40:
        return 1
    return 0

def store_retail_sentiment_snapshot(symbols: list):
    """Store each pair's snapshot in retail_sentiment_history (append-only)."""
    for s in symbols:
        pair = s.get('pair', '').replace('/', '')  # e.g., "EUR/USD" -> "EURUSD"
        if not pair:
            continue
        long_pct = float(s.get('longPercentage', 50))
        short_pct = float(s.get('shortPercentage', 50))
        long_pos = int(s.get('longPositions', 0))
        short_pos = int(s.get('shortPositions', 0))
        bias = normalize_retail_bias(long_pct)
        # Prevent duplicate timestamps – use current UTC time
        ts = datetime.utcnow().replace(microsecond=0)
        # Check if an entry for this pair with the exact timestamp already exists (optional)
        existing = RetailSentimentHistory.query.filter_by(pair=pair, timestamp=ts).first()
        if existing:
            continue
        record = RetailSentimentHistory(
            pair=pair,
            long_percentage=long_pct,
            short_percentage=short_pct,
            long_positions=long_pos,
            short_positions=short_pos,
            retail_bias=bias,
            timestamp=ts
        )
        db.session.add(record)
    db.session.commit()

def update_retail_sentiment():
    """Fetch API and store snapshot. Called by scheduler."""
    symbols = fetch_retail_sentiment_from_api()
    if symbols:
        store_retail_sentiment_snapshot(symbols)
        print(f"Retail sentiment updated at {datetime.utcnow()}")
    else:
        print("Failed to fetch retail sentiment")

def get_latest_retail_sentiment(pair: str) -> dict:
    """Return the most recent sentiment data for a pair."""
    record = RetailSentimentHistory.query.filter_by(pair=pair.upper().replace('/', '')).order_by(RetailSentimentHistory.timestamp.desc()).first()
    if not record:
        return None
    return {
        'pair': record.pair,
        'long_percentage': record.long_percentage,
        'short_percentage': record.short_percentage,
        'long_positions': record.long_positions,
        'short_positions': record.short_positions,
        'retail_bias': record.retail_bias,
        'timestamp': record.timestamp.isoformat()
    }

def get_all_latest_retail_sentiment() -> list:
    """Get latest snapshot for each pair (using DISTINCT ON in SQL, but here we do it in Python)."""
    records = RetailSentimentHistory.query.order_by(RetailSentimentHistory.pair, RetailSentimentHistory.timestamp.desc()).all()
    latest = {}
    for r in records:
        if r.pair not in latest:
            latest[r.pair] = r
    return [{
        'pair': r.pair,
        'long_percentage': r.long_percentage,
        'short_percentage': r.short_percentage,
        'long_positions': r.long_positions,
        'short_positions': r.short_positions,
        'retail_bias': r.retail_bias,
        'timestamp': r.timestamp.isoformat()
    } for r in latest.values()]


def get_retail_sentiment_score(pair: str) -> int:
    """
    Return contrarian sentiment score based on latest Myfxbook data.
    If no data available, fall back to manual SentimentData table.
    """
    # Try to get latest from Myfxbook
    data = get_latest_retail_sentiment(pair)
    if data:
        return data['retail_bias']   # already -1,0,1 (contrarian)
    # Fallback to manual entry (original logic)
    sent = SentimentData.query.filter_by(pair=pair).first()
    if not sent:
        return 0
    if sent.short_pct > sent.long_pct:
        return 1
    elif sent.long_pct > sent.short_pct:
        return -1
    return 0

def get_seasonality_directional_score(pair: str) -> int:
    bias, _ = get_seasonality_bias(pair)
    if bias == "Bullish":
        return 1
    elif bias == "Bearish":
        return -1
    return 0

def get_technical_directional_score(pair_symbol: str) -> int:
    """Return +2 if price > 21d SMA, -2 if below, 0 otherwise."""
    val = get_trend_score_21d(pair_symbol)   # returns -1,0,1
    if val == 1:
        return 2
    elif val == -1:
        return -2
    return 0

def calculate_weighted_pair_score(pair: str, base: str, quote: str) -> dict:
    """
    Returns dict with:
        total_raw: sum of contributions (range -20..20)
        category_scores: (unused but kept for compatibility)
        breakdown: {indicator_name: contribution}
    """
    base_scores = get_currency_indicator_scores(base)
    quote_scores = get_currency_indicator_scores(quote)

    breakdown = {}
    total = 0

    # All economic indicators (from WEIGHTS keys except virtual ones)
    economic_indicators = [
        "CPI YoY (%)", "PCE YoY (%)", "PPI YoY (%)",
        "GDP Growth Rate QoQ (%)", "Services PMI", "Manufacturing PMI",
        "Retail Sales MoM (%)", "Consumer Confidence",
        "NFP (K)", "Average Hourly Earnings", "Unemployment Rate (%)",
        "JOLTS Job Openings (M)", "ADP (K)", "Unemployment Claims (K)",
        "Interest Rate Decision"
    ]

    for ind_name in economic_indicators:
        base_val = base_scores.get(ind_name, 0)
        quote_val = quote_scores.get(ind_name, 0)
        pair_norm = normalize_pair_score(base_val, quote_val)  # -1,0,1
        breakdown[ind_name] = pair_norm
        total += pair_norm

    # COT Alignment (±2 or 0)
    cot_score = get_cot_alignment_score(base, quote)
    breakdown["COT_ALIGNMENT"] = cot_score
    total += cot_score

    # Retail Sentiment (±1)
    retail_score = get_retail_sentiment_score(pair)  # already -1,0,1
    breakdown["RETAIL_SENTIMENT"] = retail_score
    total += retail_score

    # Technical (±2)
    yf_symbol = SYMBOL_MAPPING.get(pair, pair.replace('/', '') + '=X')
    tech_score = get_technical_directional_score(yf_symbol)
    breakdown["TREND_21D_SMA"] = tech_score
    total += tech_score

    # Seasonality (±1)
    seas_score = get_seasonality_directional_score(pair)
    breakdown["SEASONALITY"] = seas_score
    total += seas_score

    # Clamp to ±20
    total = max(-MAX_RAW_SCORE, min(MAX_RAW_SCORE, total))

    return {
        "total_raw": total,
        "category_scores": {},   # not used
        "breakdown": breakdown
    }

def get_asset_score(asset: str) -> float:
    """
    Returns the overall score for a single asset (USD, EUR, XAU, BTC, etc.)
    using the same logic as /api/asset_scorecard/<symbol>.
    """
    base = asset.upper()
    quote = None
    is_standalone = base in ["XAU", "BTC"]

    # Technical (21-day SMA)
    yf_symbol = SYMBOL_MAPPING.get(asset, asset + '=X')
    technical_score = get_trend_score_21d(yf_symbol)

    if is_standalone:
        # Standalone assets (XAU, BTC)
        base_cot = COTData.query.filter_by(currency=base).first()
        cot_net = base_cot.net_position if base_cot else 0
        cot_change = base_cot.weekly_change if base_cot else 0
        cot_score = 1 if cot_net > 0 else (-1 if cot_net < 0 else 0)
        momentum_score = 1 if cot_change > 0 else (-1 if cot_change < 0 else 0)
        sentiment_cot_score = cot_score + momentum_score

        # Fundamentals (economic indicators + bond yield)
        category_data = {}
        for ind in EconomicIndicator.query.filter_by(currency=base).all():
            if ind.forecast == 0 and ind.actual == 0:
                continue
            cat = ind.category or 'General'
            surprise = ind.actual - ind.forecast
            if ind.is_lower_better:
                surprise = -surprise
            score_val = 1 if surprise > 0 else (-1 if surprise < 0 else 0)
            if base == "BTC" and cat == "Inflation Bias":
                score_val = -score_val
            category_data.setdefault(cat, []).append(score_val)

        bond_score = get_us_2yr_bond_trend_score()
        category_data.setdefault('Inflation Bias', []).append(bond_score)

        category_bias = {}
        for cat, scores in category_data.items():
            if scores:
                avg = sum(scores) / len(scores)
                category_bias[cat] = round(avg * len(scores), 1)
        fundamentals_score = round(sum(category_bias.values()), 1)

        # Sentiment (contrarian)
        sent = SentimentData.query.filter_by(pair=asset).first()
        if sent:
            sent_score_val = 2 if sent.short_pct > sent.long_pct else (-2 if sent.long_pct > sent.short_pct else 0)
        else:
            sent_score_val = 0

        season_bias, season_score = get_seasonality_bias(asset)

        tradion_score = technical_score + sentiment_cot_score + fundamentals_score
        overall_score = tradion_score + season_score + sent_score_val

    else:
        # Normal currencies (USD, EUR, GBP...)
        # For a single currency, we treat it as the "base" with no quote.
        # However, the asset scorecard API actually uses only the base side.
        # To keep consistency, we reuse the standalone‑like logic but without COT/indicator subtraction.
        # Simpler: fetch the asset's own indicators and COT directly.
        base_cot = COTData.query.filter_by(currency=base).first()
        cot_net = base_cot.net_position if base_cot else 0
        cot_change = base_cot.weekly_change if base_cot else 0
        cot_score = 1 if cot_net > 0 else (-1 if cot_net < 0 else 0)
        momentum_score = 1 if cot_change > 0 else (-1 if cot_change < 0 else 0)
        sentiment_cot_score = cot_score + momentum_score

        category_data = {}
        for ind in EconomicIndicator.query.filter_by(currency=base).all():
            if ind.forecast == 0 and ind.actual == 0:
                continue
            cat = ind.category or 'General'
            surprise = ind.actual - ind.forecast
            if ind.is_lower_better:
                surprise = -surprise
            score_val = 1 if surprise > 0 else (-1 if surprise < 0 else 0)
            category_data.setdefault(cat, []).append(score_val)

        bond_score = get_us_2yr_bond_trend_score()
        category_data.setdefault('Inflation Bias', []).append(bond_score)

        category_bias = {}
        for cat, scores in category_data.items():
            if scores:
                avg = sum(scores) / len(scores)
                category_bias[cat] = round(avg * len(scores), 1)
        fundamentals_score = round(sum(category_bias.values()), 1)

        sent = SentimentData.query.filter_by(pair=asset).first()
        sent_score_val = 2 if sent and sent.short_pct > sent.long_pct else (-2 if sent and sent.long_pct > sent.short_pct else 0)

        season_bias, season_score = get_seasonality_bias(asset)

        tradion_score = technical_score + sentiment_cot_score + fundamentals_score
        overall_score = tradion_score + season_score + sent_score_val

    return overall_score


def classify_bias(score: float) -> tuple:
    """Return (bias_string, symbol, color, display_score) based on ±20 scale."""
    if score >= 16:
        return "EXTREME BULLISH", "🔥🔥🔥", "#00e5a0", score
    elif score >= 10:
        return "VERY BULLISH", "🔥🔥", "#00cc88", score
    elif score >= 5:
        return "BULLISH", "🔥", "#00aa66", score
    elif score <= -16:
        return "EXTREME BEARISH", "💀💀💀", "#ff4d6d", score
    elif score <= -10:
        return "VERY BEARISH", "💀💀", "#ff6688", score
    elif score <= -5:
        return "BEARISH", "💀", "#ff8099", score
    else:
        return "NEUTRAL", "⚡", "#ffaa00", score

def get_economic_weighted_bias(base: str, quote: str) -> tuple:
    
    """Returns (bias_string, symbol, diff, econ_score_normalized) for dashboard."""
    base_scores = get_currency_indicator_scores(base)
    quote_scores = get_currency_indicator_scores(quote)

    economic_indicators = [
        "CPI YoY (%)", "PCE YoY (%)", "PPI YoY (%)",
        "GDP Growth Rate QoQ (%)", "Services PMI", "Manufacturing PMI",
        "Retail Sales MoM (%)", "Consumer Confidence",
        "NFP (K)", "Average Hourly Earnings", "Unemployment Rate (%)",
        "JOLTS Job Openings (M)", "ADP (K)", "Unemployment Claims (K)"
    ]
    total_raw = 0
    for ind in economic_indicators:
        w = WEIGHTS.get(ind, 0)
        if w == 0:
            continue
        base_val = base_scores.get(ind, 0)
        quote_val = quote_scores.get(ind, 0)
        pair_norm = normalize_pair_score(base_val, quote_val)
        total_raw += pair_norm * w

        total_raw = max(-MAX_RAW_SCORE, min(MAX_RAW_SCORE, total_raw))

    # Map to bias string for economic section (same thresholds as overall)
    if total_raw >= 16:
        bias, sym = "EXTREME BULLISH", "▲▲▲"
    elif total_raw >= 10:
        bias, sym = "VERY BULLISH", "▲▲"
    elif total_raw >= 5:
        bias, sym = "BULLISH", "▲"
    elif total_raw <= -16:
        bias, sym = "EXTREME BEARISH", "▼▼▼"
    elif total_raw <= -10:
        bias, sym = "VERY BEARISH", "▼▼"
    elif total_raw <= -5:
        bias, sym = "BEARISH", "▼"
    else:
        bias, sym = "NEUTRAL", "●"

    # Normalized score for old econ_score (range -4..4)
    econ_score_norm = round(total_raw / 9.5)  # roughly map -38..38 to -4..4
    econ_score_norm = max(-4, min(4, econ_score_norm))

    return bias, sym, total_raw, econ_score_norm

# -----------------------------
# COT ANALYSIS
# -----------------------------
def analyze_cot(pairs):
    currency_data = {}
    cot_records = COTData.query.all()
    for rec in cot_records:
        currency_data[rec.currency.upper()] = {
            "net": rec.net_position,
            "change": rec.weekly_change,
            "longs": rec.longs,
            "shorts": rec.shorts
        }

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
# HISTORICAL COT FUNCTIONS
# -----------------------------
def save_cot_snapshot(currency: str, report_date, longs: float, shorts: float, net: float, weekly_change: float):
    """Save a COT snapshot, avoiding duplicates.
       report_date can be a date object or string; we convert to date."""
    from datetime import date, datetime
    if isinstance(report_date, str):
        report_date = datetime.strptime(report_date, '%Y-%m-%d').date()
    elif isinstance(report_date, datetime):
        report_date = report_date.date()
    # else assume it's already a date object
    existing = COTHistory.query.filter_by(currency=currency.upper(), report_date=report_date).first()
    if existing:
        return
    bias = 1 if net > 0 else (-1 if net < 0 else 0)
    snapshot = COTHistory(
        currency=currency.upper(),
        report_date=report_date,
        long_positions=longs,
        short_positions=shorts,
        net_positions=net,
        weekly_change=weekly_change,
        bias=bias
    )
    db.session.add(snapshot)
    db.session.commit()

def get_historical_cot(currency: str, limit: int = 30):
    """Return list of historical COT records for a currency, ordered by date."""
    records = COTHistory.query.filter_by(currency=currency.upper()).order_by(COTHistory.report_date.asc()).limit(limit).all()
    return [{
        'date': r.report_date.isoformat(),
        'net': r.net_positions,
        'weekly_change': r.weekly_change,
        'bias': r.bias,
        'longs': r.long_positions,
        'shorts': r.short_positions
    } for r in records]

def get_historical_cot_with_changes(currency: str, limit: int = 50):
    records = COTHistory.query.filter_by(currency=currency.upper()).order_by(COTHistory.report_date.asc()).limit(limit).all()
    result = []
    for r in records:
        item = {
            'id': r.id,
            'date': r.report_date.isoformat(),
            'longs': r.long_positions,
            'shorts': r.short_positions,
            'net': r.net_positions,
            'change_longs': r.change_longs if r.change_longs != 0 else None,
            'change_shorts': r.change_shorts if r.change_shorts != 0 else None,
            'change_net': r.weekly_change if r.weekly_change != 0 else None
        }
        result.append(item)
    return result

def get_latest_cot(currency: str):
    """Return the most recent COT snapshot for a currency."""
    record = COTHistory.query.filter_by(currency=currency.upper()).order_by(COTHistory.report_date.desc()).first()
    if not record:
        return None
    return {
        'date': record.report_date.isoformat(),
        'net': record.net_positions,
        'weekly_change': record.weekly_change,
        'bias': record.bias
    }

def get_cot_trend(currency: str, periods: int = 5):
    """Analyze trend of net positions over last N periods (simple direction)."""
    records = COTHistory.query.filter_by(currency=currency.upper()).order_by(COTHistory.report_date.desc()).limit(periods).all()
    if len(records) < 2:
        return 'neutral'
    first_net = records[-1].net_positions
    last_net = records[0].net_positions
    if last_net > first_net:
        return 'increasing'
    elif last_net < first_net:
        return 'decreasing'
    return 'stable'

# -----------------------------
# ECONOMIC ANALYSIS
# -----------------------------
def is_lower_better(name):
    return any(k.lower() in str(name).lower() for k in LOWER_BETTER_INDICATORS)

def analyze_currency_econ(currency):
    indicators = EconomicIndicator.query.filter_by(currency=currency.upper()).all()
    if not indicators:
        return 50
    bullish = bearish = 0
    for ind in indicators:
        name = ind.indicator_name
        if not name:
            continue
        if "Employment Change" in name:
            continue
        if ind.forecast == 0 and ind.actual == 0:
            continue

        # Use the database flag (this fixes the heatmap)
        lower_better = ind.is_lower_better

        # Determine normal bullish/bearish
        if lower_better:
            normal_bullish = (ind.actual < ind.forecast)
            normal_bearish = (ind.actual > ind.forecast)
        else:
            normal_bullish = (ind.actual > ind.forecast)
            normal_bearish = (ind.actual < ind.forecast)

        # For BTC, invert Inflation Bias category
        invert = False
        if currency.upper() == "BTC" and ind.category == "Inflation Bias":
            invert = True

        if invert:
            # Flip bullish and bearish
            if normal_bullish:
                bearish += 1
            elif normal_bearish:
                bullish += 1
        else:
            if normal_bullish:
                bullish += 1
            elif normal_bearish:
                bearish += 1

    total = bullish + bearish
    if total == 0:
        return 50
    return (bullish / total) * 100

def get_indicator_contribution(currency, indicator_name):
    """Return scaled contribution: ±2 for most, ±1 for jobs, inflation, and Core PCE."""
    ind = EconomicIndicator.query.filter_by(
        currency=currency.upper(),
        indicator_name=indicator_name
    ).first()
    if not ind or (ind.forecast == 0 and ind.actual == 0):
        return 0
    if "Employment Change" in ind.indicator_name:
        return 0

    # Hardcoded job market indicators (ignore database flags)
    higher_better_jobs = ["NFP (K)", "ADP (K)", "JOLTS Job Openings (M)"]
    lower_better_jobs = ["Unemployment Rate (%)", "Unemployment Claims (K)"]
    # Inflation indicators that should have scale ±1
    inflation_indicators = ["CPI YoY (%)", "PPI YoY (%)", "Core PCE YoY (%)"]

    # Determine raw contribution (±1,0)
    if indicator_name in higher_better_jobs:
        if ind.actual > ind.forecast:
            raw = 1
        elif ind.actual < ind.forecast:
            raw = -1
        else:
            raw = 0
    elif indicator_name in lower_better_jobs:
        if ind.actual < ind.forecast:
            raw = 1
        elif ind.actual > ind.forecast:
            raw = -1
        else:
            raw = 0
    else:
        lower_better = ind.is_lower_better
        if lower_better:
            if ind.actual < ind.forecast:
                raw = 1
            elif ind.actual > ind.forecast:
                raw = -1
            else:
                raw = 0
        else:
            if ind.actual > ind.forecast:
                raw = 1
            elif ind.actual < ind.forecast:
                raw = -1
            else:
                raw = 0

    # Scaling: ±1 for job market, inflation, and Core PCE; ±2 for others
    job_market = higher_better_jobs + lower_better_jobs
    if indicator_name in job_market or indicator_name in inflation_indicators:
        return raw
    else:
        return raw * 2

CATEGORY_WEIGHTS = {
    'Technical Bias': 1.0,
    'Economic Growth Bias': 1.0,
    'Inflation Bias': 1.0,
    'Jobs Market Bias': 1.0,
    'Crowd Sentiment (COT)': 1.0,
    'General': 1.0
}

def calculate_currency_composite_score(currency):
    indicators = EconomicIndicator.query.filter_by(currency=currency.upper()).all()
    if not indicators:
        return 0
    category_scores = {}
    category_counts = {}
    for ind in indicators:
        if ind.forecast == 0 and ind.actual == 0:
            continue
        surprise = ind.actual - ind.forecast
        if ind.is_lower_better:
            surprise = -surprise
        
        # For BTC, invert inflation-related categories
        contribution = 1 if surprise > 0 else (-1 if surprise < 0 else 0)
        cat = ind.category or 'General'
        
        # Invert Inflation Bias for BTC
        if currency.upper() == "BTC" and cat == "Inflation Bias":
            contribution = -contribution
        
        category_scores[cat] = category_scores.get(cat, 0) + contribution
        category_counts[cat] = category_counts.get(cat, 0) + 1

    total_weighted = 0
    total_weights = 0
    for cat, sum_score in category_scores.items():
        count = category_counts[cat]
        if count == 0:
            continue
        avg_cat = sum_score / count
        weight = CATEGORY_WEIGHTS.get(cat, 1.0)
        total_weighted += avg_cat * weight
        total_weights += weight

    if total_weights == 0:
        return 0
    composite = total_weighted / total_weights
    return round(composite * 10, 2)

def get_econ_pair_bias(base, quote):
    # Get heatmap percentages (0-100)
    base_pct = analyze_currency_econ(base)
    quote_pct = analyze_currency_econ(quote)
    
    # Difference = base% - quote% (positive means base more bullish)
    diff = base_pct - quote_pct
    
    # Invert for XAU/USD and BTC/USD (these pairs move opposite to USD)
    if (base == "XAU" and quote == "USD") or (base == "BTC" and quote == "USD"):
        diff = -diff
    
    # Map absolute difference to score (0 to 4, with sign)
    abs_diff = abs(diff)
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
    
    # Apply sign
    if diff < 0:
        econ_score = -econ_score
    
    # Generate bias string and symbol (for display)
    if econ_score >= 4:
        bias = "EXTREME BULLISH"
        sym = "▲▲▲"
    elif econ_score >= 3:
        bias = "VERY BULLISH"
        sym = "▲▲"
    elif econ_score >= 2:
        bias = "BULLISH"
        sym = "▲"
    elif econ_score >= 1:
        bias = "SLIGHTLY BULLISH"
        sym = "▲"
    elif econ_score <= -4:
        bias = "EXTREME BEARISH"
        sym = "▼▼▼"
    elif econ_score <= -3:
        bias = "VERY BEARISH"
        sym = "▼▼"
    elif econ_score <= -2:
        bias = "BEARISH"
        sym = "▼"
    elif econ_score <= -1:
        bias = "SLIGHTLY BEARISH"
        sym = "▼"
    else:
        bias = "NEUTRAL"
        sym = "●"
    
    return bias, sym, diff, econ_score

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

def save_asset_score_history(asset: str, score: float):
    """Save score – if already saved today, just update it."""
    try:
        today = datetime.utcnow().date()
        # Start of today (00:00:00) and end of today (23:59:59.999999)
        start_of_day = datetime.combine(today, datetime.min.time())
        end_of_day = datetime.combine(today, datetime.max.time())
        
        # Find an existing record for this asset recorded today
        existing = AssetScoreHistory.query.filter(
            AssetScoreHistory.asset == asset,
            AssetScoreHistory.recorded_at >= start_of_day,
            AssetScoreHistory.recorded_at <= end_of_day
        ).first()
        
        if existing:
            # Update today's record
            existing.score = score
            existing.recorded_at = datetime.utcnow()
        else:
            # Add new record
            new_entry = AssetScoreHistory(asset=asset, score=score, recorded_at=datetime.utcnow())
            db.session.add(new_entry)
        
        db.session.commit()
        
        # Keep only last 30 records (delete oldest if over 30)
        count = AssetScoreHistory.query.filter_by(asset=asset).count()
        if count > 30:
            oldest = AssetScoreHistory.query.filter_by(asset=asset).order_by(AssetScoreHistory.recorded_at.asc()).first()
            if oldest:
                db.session.delete(oldest)
                db.session.commit()
    except Exception as e:
        print(f"Error saving score history for {asset}: {e}")


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
@app.route('/api/detailed_analysis')
@login_required
def detailed_analysis():
    # ---------- PREFETCH ALL DATA (ONCE PER REQUEST) ----------
    all_indicators = EconomicIndicator.query.all()
    econ_cache = {}
    for ind in all_indicators:
        if ind.forecast == 0 and ind.actual == 0:
            continue
        if "Employment Change" in ind.indicator_name:
            continue
        curr = ind.currency.upper()
        if ind.indicator_name == "Interest Rate Decision":
            score = 1 if ind.actual > ind.forecast else (-1 if ind.actual < ind.forecast else 0)
        else:
            if ind.is_lower_better:
                score = 1 if ind.actual < ind.forecast else (-1 if ind.actual > ind.forecast else 0)
            else:
                score = 1 if ind.actual > ind.forecast else (-1 if ind.actual < ind.forecast else 0)
        econ_cache.setdefault(curr, {})[ind.indicator_name] = score

    cot_records = COTData.query.all()
    cot_cache = {c.currency.upper(): {'net': c.net_position, 'change': c.weekly_change} for c in cot_records}

    # ---------- NEW COT ALIGNMENT (sum‑difference, clamped) ----------
    def get_cot_align(base, quote):
        base_cot = cot_cache.get(base, {'net':0, 'change':0})
        quote_cot = cot_cache.get(quote, {'net':0, 'change':0})
        base_net_dir = 1 if base_cot['net'] > 0 else (-1 if base_cot['net'] < 0 else 0)
        base_mom_dir = 1 if base_cot['change'] > 0 else (-1 if base_cot['change'] < 0 else 0)
        quote_net_dir = 1 if quote_cot['net'] > 0 else (-1 if quote_cot['net'] < 0 else 0)
        quote_mom_dir = 1 if quote_cot['change'] > 0 else (-1 if quote_cot['change'] < 0 else 0)
        base_score = base_net_dir + base_mom_dir
        quote_score = quote_net_dir + quote_mom_dir
        raw_diff = base_score - quote_score
        if raw_diff > 2:
            return 2
        elif raw_diff < -2:
            return -2
        else:
            return raw_diff

    results = []
    for base, quote in ALL_PAIRS:
        pair = f"{base}/{quote}"
        is_standalone = base in ["XAU", "BTC"] and quote == "USD"
        try:
            if is_standalone:
                base_scores = econ_cache.get(base, {})
                def raw(name):
                    return base_scores.get(name, 0)
                gdp = raw("GDP Growth Rate QoQ (%)")
                m_pmi = raw("Manufacturing PMI")
                s_pmi = raw("Services PMI")
                retail = raw("Retail Sales MoM (%)")
                consumer_conf = raw("Consumer Confidence")
                cpi = raw("CPI YoY (%)")
                ppi = raw("PPI YoY (%)")
                pce = raw("PCE YoY (%)")
                interest = raw("Interest Rate Decision")
                nfp = raw("NFP (K)")
                avg_hourly = raw("Average Hourly Earnings")
                unemp_rate = raw("Unemployment Rate (%)")
                unemp_claims = raw("Unemployment Claims (K)")
                adp = raw("ADP (K)")
                jolts = raw("JOLTS Job Openings (M)")

                base_cot = cot_cache.get(base, {'net':0, 'change':0})
                cot_net_dir = 1 if base_cot['net'] > 0 else (-1 if base_cot['net'] < 0 else 0)
                cot_mom_dir = 1 if base_cot['change'] > 0 else (-1 if base_cot['change'] < 0 else 0)
                cot_align = 2 if (cot_net_dir > 0 and cot_mom_dir > 0) else (-2 if (cot_net_dir < 0 and cot_mom_dir < 0) else 0)

                crowd = get_retail_sentiment_score(pair)
                yf_symbol = SYMBOL_MAPPING.get(pair, pair.replace('/', '') + '=X')
                trend = get_technical_directional_score(yf_symbol)
                season = get_seasonality_directional_score(pair)

                total = (gdp + m_pmi + s_pmi + retail + consumer_conf +
                         cpi + ppi + pce + interest + nfp + avg_hourly +
                         unemp_rate + unemp_claims + adp + jolts +
                         cot_align + crowd + trend + season)
                total = max(-20, min(20, total))
                bias, _, _, _ = classify_bias(total)

                results.append({
                    "symbol": pair, "bias": bias, "score": round(total,1),
                    "trend": trend, "seasonality": season, "cot": cot_align, "crowd": crowd,
                    "gdp": gdp, "m_pmi": m_pmi, "s_pmi": s_pmi, "retail": retail,
                    "consumer_conf": consumer_conf, "cpi": cpi, "ppi": ppi, "pce": pce,
                    "interest": interest, "nfp": nfp, "avg_hourly_earnings": avg_hourly,
                    "unemp_rate": unemp_rate, "unemp_claims": unemp_claims, "adp": adp, "jolts": jolts
                })
            else:
                base_scores = econ_cache.get(base, {})
                quote_scores = econ_cache.get(quote, {})
                def norm(name):
                    b = base_scores.get(name, 0)
                    q = quote_scores.get(name, 0)
                    return 1 if b - q > 0 else (-1 if b - q < 0 else 0)
                gdp = norm("GDP Growth Rate QoQ (%)")
                m_pmi = norm("Manufacturing PMI")
                s_pmi = norm("Services PMI")
                retail = norm("Retail Sales MoM (%)")
                consumer_conf = norm("Consumer Confidence")
                cpi = norm("CPI YoY (%)")
                ppi = norm("PPI YoY (%)")
                pce = norm("PCE YoY (%)")
                interest = norm("Interest Rate Decision")
                nfp = norm("NFP (K)")
                avg_hourly = norm("Average Hourly Earnings")
                unemp_rate = norm("Unemployment Rate (%)")
                unemp_claims = norm("Unemployment Claims (K)")
                adp = norm("ADP (K)")
                jolts = norm("JOLTS Job Openings (M)")

                cot_align = get_cot_align(base, quote)   # NEW function used here
                crowd = get_retail_sentiment_score(pair)
                yf_symbol = SYMBOL_MAPPING.get(pair, pair.replace('/', '') + '=X')
                trend = get_technical_directional_score(yf_symbol)
                season = get_seasonality_directional_score(pair)

                total = (gdp + m_pmi + s_pmi + retail + consumer_conf +
                         cpi + ppi + pce + interest + nfp + avg_hourly +
                         unemp_rate + unemp_claims + adp + jolts +
                         cot_align + crowd + trend + season)
                total = max(-20, min(20, total))
                bias, _, _, _ = classify_bias(total)

                results.append({
                    "symbol": pair, "bias": bias, "score": round(total,1),
                    "trend": trend, "seasonality": season, "cot": cot_align, "crowd": crowd,
                    "gdp": gdp, "m_pmi": m_pmi, "s_pmi": s_pmi, "retail": retail,
                    "consumer_conf": consumer_conf, "cpi": cpi, "ppi": ppi, "pce": pce,
                    "interest": interest, "nfp": nfp, "avg_hourly_earnings": avg_hourly,
                    "unemp_rate": unemp_rate, "unemp_claims": unemp_claims, "adp": adp, "jolts": jolts
                })
        except Exception as e:
            print(f"Error processing {pair}: {e}")
            results.append({
                "symbol": pair, "bias": "ERROR", "score": 0,
                "trend": 0, "seasonality": 0, "cot": 0, "crowd": 0,
                "gdp": 0, "m_pmi": 0, "s_pmi": 0, "retail": 0, "consumer_conf": 0,
                "cpi": 0, "ppi": 0, "pce": 0, "interest": 0,
                "nfp": 0, "avg_hourly_earnings": 0, "unemp_rate": 0,
                "unemp_claims": 0, "adp": 0, "jolts": 0
            })
    return jsonify(results)

@app.route('/api/analyze', methods=['POST'])
@login_required
def api_analyze():
    global cached_analysis, cached_heatmap, last_analysis_time, manual_refresh_triggered
    try:
        user = User.query.get(session['user_id'])

        # Force fresh calculation if needed
        if manual_refresh_triggered or cached_analysis is None or last_analysis_time is None:
            manual_refresh_triggered = False
        else:
            time_diff = (datetime.now() - last_analysis_time).total_seconds()
            if time_diff < 300 and cached_analysis is not None:
                return jsonify({'results': cached_analysis, 'heatmap': cached_heatmap, 'cached': True})

        results = []
        sentiment_data = {s.pair: s for s in SentimentData.query.all()}

        for base, quote in ALL_PAIRS:
            pair_name = f"{base}/{quote}"

            # ---- NEW WEIGHTED SCORES ----
            weighted = calculate_weighted_pair_score(pair_name, base, quote)
            total_score = weighted["total_raw"]          # FIXED: use total_raw, not total_scaled
            overall_bias, overall_symbol, overall_color, display_score = classify_bias(total_score)

            # ---- COT data for display (keep old format) ----
            cot_df, _ = analyze_cot([(base, quote)])
            if not cot_df.empty:
                r = cot_df.iloc[0]
                cot_data = {
                    'bias': str(r['COT_Bias']),
                    'symbol': str(r['COT_Symbol']),
                    'momentum': str(r['Momentum']),
                    'mom_symbol': str(r['Mom_Symbol']),
                    'score': int(r['COT_Score']),
                    'momentum_score': int(r['Momentum_Score'])
                }
            else:
                cot_data = {'bias': 'No Data', 'symbol': '●', 'momentum': 'No Data',
                            'mom_symbol': '●', 'score': 0, 'momentum_score': 0}

            # ---- Economic display using new weighted economic bias ----
            econ_bias, econ_sym, diff_econ, econ_score_norm = get_economic_weighted_bias(base, quote)
            econ_data = {
                'bias': econ_bias,
                'symbol': econ_sym,
                'diff': diff_econ,
                'score': econ_score_norm
            }

            # ---- Technical (21d SMA) ----
            yf_symbol = SYMBOL_MAPPING.get(pair_name, pair_name.replace('/', '') + '=X')
            trend_raw = get_technical_directional_score(yf_symbol)
            trend_bias = "Bullish" if trend_raw > 0 else "Bearish" if trend_raw < 0 else "Neutral"
            trend_symbol = "▲" if trend_raw > 0 else "▼" if trend_raw < 0 else "●"

            # ---- Seasonality ----
            seasonality_bias, seasonality_score = get_seasonality_bias(pair_name)

            # ---- Sentiment (contrarian) ----
            sent = sentiment_data.get(pair_name)
            if sent:
                if sent.short_pct > sent.long_pct:
                    sentiment_label = "Bullish"
                    sent_score = 2
                elif sent.long_pct > sent.short_pct:
                    sentiment_label = "Bearish"
                    sent_score = -2
                else:
                    sentiment_label = "Neutral"
                    sent_score = 0
            else:
                sentiment_label = "N/A"
                sent_score = 0

            # ---- Build result object ----
            result = {
                'pair': pair_name,
                'cot': cot_data,
                'economic': econ_data,
                'trend': {'bias': trend_bias, 'symbol': trend_symbol, 'score': trend_raw},
                'seasonality': {'bias': seasonality_bias, 'score': seasonality_score},
                'sentiment': {'bias': sentiment_label, 'score': sent_score},
                'overall': {
                    'bias': overall_bias,
                    'symbol': overall_symbol,
                    'score': round(display_score, 1),
                    'color': overall_color
                },
            }
            results.append(result)

            # Save history
            history = AnalysisHistory(
                user_id=user.id,
                pair=pair_name,
                cot_bias=str(cot_data['bias']),
                cot_momentum=str(cot_data['momentum']),
                econ_bias=econ_bias,
                econ_diff=diff_econ,
                trend_score=float(trend_raw),
                seasonality_bias=seasonality_bias,
                seasonality_score=float(seasonality_score),
                overall_score=float(total_score),
                overall_bias=overall_bias
            )
            db.session.add(history)

        db.session.commit()

        # Currency strength (keep legacy calculation)
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
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500


@app.route('/debug/indicator_cache/<currency>')
@login_required
@admin_required
def debug_indicator_cache(currency):
    currency = currency.upper()
    indicators = EconomicIndicator.query.filter_by(currency=currency).all()
    cache = {}
    for ind in indicators:
        if ind.forecast == 0 and ind.actual == 0:
            continue
        if "Employment Change" in ind.indicator_name:
            continue
        if ind.indicator_name == "Interest Rate Decision":
            score = 1 if ind.actual > ind.forecast else (-1 if ind.actual < ind.forecast else 0)
        else:
            if ind.is_lower_better:
                score = 1 if ind.actual < ind.forecast else (-1 if ind.actual > ind.forecast else 0)
            else:
                score = 1 if ind.actual > ind.forecast else (-1 if ind.actual < ind.forecast else 0)
        cache[ind.indicator_name] = {'forecast': ind.forecast, 'actual': ind.actual, 'score': score}
    return jsonify(cache)



@app.route('/api/asset_scorecard/<path:symbol>')
@login_required
def api_asset_scorecard(symbol):
    parts = symbol.split('/')
    if len(parts) == 2:
        base, quote = parts[0].upper(), parts[1].upper()
    else:
        base = symbol.upper()
        quote = None

    # Determine if this is a standalone asset (XAU or BTC)
    is_standalone = base in ["XAU", "BTC"] and quote == "USD"
    
    # ----- STANDALONE ASSET LOGIC (XAU/BTC) -----
    if is_standalone:
        # Technical score for standalone (XAU, BTC) – 21-day SMA
        yf_symbol = SYMBOL_MAPPING.get(symbol, symbol.replace('/', '') + '=X')
        technical_score = get_trend_score_21d(yf_symbol)
        
        # COT: net direction + momentum direction (each ±1 → total -2..+2)
        base_cot = COTData.query.filter_by(currency=base).first()
        net_dir = 1 if base_cot and base_cot.net_position > 0 else (-1 if base_cot and base_cot.net_position < 0 else 0)
        mom_dir = 1 if base_cot and base_cot.weekly_change > 0 else (-1 if base_cot and base_cot.weekly_change < 0 else 0)
        sentiment_cot_score = net_dir + mom_dir
        
        # Fundamentals: Use asset's own indicators only (no USD)
        base_indicators = EconomicIndicator.query.filter_by(currency=base).all()
        category_data = {}
        for ind in base_indicators:
            if ind.forecast == 0 and ind.actual == 0:
                continue
            cat = ind.category or 'General'
            surprise = ind.actual - ind.forecast
            if ind.is_lower_better:
                surprise = -surprise
            score = 1 if surprise > 0 else (-1 if surprise < 0 else 0)
            
            # Special inversion for BTC Inflation Bias
            if base == "BTC" and cat == "Inflation Bias":
                score = -score
                
            category_data.setdefault(cat, []).append(score)
        
        # --- Add US 2-year bond trend indicator (score only) ---
        bond_score = get_us_2yr_bond_trend_score()
        category_data.setdefault('Inflation Bias', []).append(bond_score)
        
        category_bias = {}
        for cat, scores in category_data.items():
            if scores:
                avg = sum(scores) / len(scores)
                category_bias[cat] = round(avg * len(scores), 1)
        
        fundamentals_score = round(sum(category_bias.values()), 1)
        
        # Seasonality (standalone)
        season_bias, season_score = get_seasonality_bias(symbol)
        
        # Sentiment (standalone) - contrarian already
        sent = SentimentData.query.filter_by(pair=symbol).first()
        if sent:
            if sent.short_pct > sent.long_pct:
                sent_score_val = 2
            elif sent.long_pct > sent.short_pct:
                sent_score_val = -2
            else:
                sent_score_val = 0
        else:
            sent_score_val = 0
        
        # Final score calculation (no USD subtraction)
        tradion_score = technical_score + sentiment_cot_score + fundamentals_score
        overall_score = tradion_score + season_score + sent_score_val
        overall_bias, overall_symbol, overall_color, display_score = get_overall_bias_and_color(overall_score)
        
        # Prepare base_indicators list for JSON (including bond and COT Alignment)
        base_indicators_json = [{
            'name': i.indicator_name,
            'forecast': i.forecast,
            'actual': i.actual,
            'is_lower_better': i.is_lower_better,
            'category': i.category or 'General'
        } for i in base_indicators]
        # Add 2-Year Bond Yield Trend
        base_indicators_json.append({
            'name': '2-Year Bond Yield Trend',
            'forecast': None,
            'actual': None,
            'is_lower_better': False,
            'category': 'Inflation Bias',
            'score': bond_score
        })
        # Add COT Alignment (for display only, not in category_data)
        base_indicators_json.append({
            'name': 'COT Alignment',
            'forecast': None,
            'actual': None,
            'is_lower_better': False,
            'category': 'Crowd Sentiment (COT)',
            'score': sentiment_cot_score
        })
        
        # Return standalone response
        return jsonify({
            'symbol': symbol,
            'base': base,
            'quote': quote if quote else 'USD',
            'overall': {
                'bias': overall_bias,
                'symbol': overall_symbol,
                'score': overall_score,
                'display_score': display_score,
                'color': overall_color
            },
            'tradion_score': tradion_score,
            'technical_score': technical_score,
            'sentiment_cot_score': sentiment_cot_score,
            'fundamentals_score': fundamentals_score,
            'category_bias': category_bias,
            'base_indicators': base_indicators_json,
            'quote_indicators': [],
            'seasonality_bias': season_bias,
            'seasonality_score': season_score,
            'sentiment_score': sent_score_val,
            'score_history': [0, 1, 2, 1, 3, 2, 4, 3, 2, 1, 2, 0]
        })
    
    # ----- NORMAL PAIR LOGIC (EUR/USD, GBP/JPY, etc.) -----
    # Technical score: compute only if this is a real pair (quote exists)
    if quote:
        yf_symbol = SYMBOL_MAPPING.get(symbol, symbol.replace('/', '') + '=X')
        technical_score = get_trend_score_21d(yf_symbol)
    else:
        technical_score = 0   # single currency (e.g., "USD") – no price series

    base_cot = COTData.query.filter_by(currency=base).first()
    quote_cot = COTData.query.filter_by(currency=quote).first() if quote else None

    # Get directional scores for base currency (-2..+2)
    base_net_dir = 1 if base_cot and base_cot.net_position > 0 else (-1 if base_cot and base_cot.net_position < 0 else 0)
    base_mom_dir = 1 if base_cot and base_cot.weekly_change > 0 else (-1 if base_cot and base_cot.weekly_change < 0 else 0)
    base_score = base_net_dir + base_mom_dir

    # Get directional scores for quote currency (-2..+2)
    quote_net_dir = 1 if quote_cot and quote_cot.net_position > 0 else (-1 if quote_cot and quote_cot.net_position < 0 else 0)
    quote_mom_dir = 1 if quote_cot and quote_cot.weekly_change > 0 else (-1 if quote_cot and quote_cot.weekly_change < 0 else 0)
    quote_score = quote_net_dir + quote_mom_dir

    # Raw pair difference ranges from -4 to +4, then clamp to -2..+2
    raw_diff = base_score - quote_score
    if raw_diff > 2:
        sentiment_cot_score = 2
    elif raw_diff < -2:
        sentiment_cot_score = -2
    else:
        sentiment_cot_score = raw_diff

    base_indicators = EconomicIndicator.query.filter_by(currency=base).all()
    category_data = {}
    for ind in base_indicators:
        if ind.forecast == 0 and ind.actual == 0:
            continue
        cat = ind.category or 'General'
        surprise = ind.actual - ind.forecast
        if ind.is_lower_better:
            surprise = -surprise
        score = 1 if surprise > 0 else (-1 if surprise < 0 else 0)
        category_data.setdefault(cat, []).append(score)

    # --- Add US 2-year bond trend indicator (score only) ---
    bond_score = get_us_2yr_bond_trend_score()
    category_data.setdefault('Inflation Bias', []).append(bond_score)

    category_bias = {}
    for cat, scores in category_data.items():
        if scores:
            avg = sum(scores) / len(scores)
            category_bias[cat] = round(avg * len(scores), 1)

    fundamentals_score = round(sum(v for k, v in category_bias.items() if k != 'Technical Bias'), 1)

    season_bias, season_score = get_seasonality_bias(symbol)

    sent = SentimentData.query.filter_by(pair=symbol).first()
    if sent:
        if sent.short_pct > sent.long_pct:
            sent_score_val = 2
        elif sent.long_pct > sent.short_pct:
            sent_score_val = -2
        else:
            sent_score_val = 0
    else:
        sent_score_val = 0

    tradion_score = technical_score + sentiment_cot_score + fundamentals_score
    overall_score = tradion_score + season_score + sent_score_val
    overall_bias, overall_symbol, overall_color, display_score = get_overall_bias_and_color(overall_score)

    # Build quote indicators list (for display)
    quote_indicators = [] if not quote else [{
        'name': i.indicator_name,
        'forecast': i.forecast,
        'actual': i.actual,
        'is_lower_better': i.is_lower_better,
        'category': i.category or 'General'
    } for i in EconomicIndicator.query.filter_by(currency=quote.upper()).all()]

    # Add bond indicator to quote side (with score field)
    if quote:
        quote_indicators.append({
            'name': '2-Year Bond Yield Trend',
            'forecast': None,
            'actual': None,
            'is_lower_better': False,
            'category': 'Inflation Bias',
            'score': bond_score
        })

    # Prepare base_indicators JSON (including bond and COT Alignment)
    base_indicators_json = [{
        'name': i.indicator_name,
        'forecast': i.forecast,
        'actual': i.actual,
        'is_lower_better': i.is_lower_better,
        'category': i.category or 'General'
    } for i in base_indicators]
    # Add 2-Year Bond Yield Trend
    base_indicators_json.append({
        'name': '2-Year Bond Yield Trend',
        'forecast': None,
        'actual': None,
        'is_lower_better': False,
        'category': 'Inflation Bias',
        'score': bond_score
    })
    # Add COT Alignment (for display only)
    base_indicators_json.append({
        'name': 'COT Alignment',
        'forecast': None,
        'actual': None,
        'is_lower_better': False,
        'category': 'Crowd Sentiment (COT)',
        'score': sentiment_cot_score
    })

    return jsonify({
        'symbol': symbol,
        'base': base,
        'quote': quote if quote else 'USD',
        'overall': {
            'bias': overall_bias,
            'symbol': overall_symbol,
            'score': overall_score,
            'display_score': display_score,
            'color': overall_color
        },
        'tradion_score': tradion_score,
        'technical_score': technical_score,
        'sentiment_cot_score': sentiment_cot_score,
        'fundamentals_score': fundamentals_score,
        'category_bias': category_bias,
        'base_indicators': base_indicators_json,
        'quote_indicators': quote_indicators,
        'seasonality_bias': season_bias,
        'seasonality_score': season_score,
        'sentiment_score': sent_score_val,
        'score_history': [0, 1, 2, 1, 3, 2, 4, 3, 2, 1, 2, 0]
    })


@app.route('/api/score_history/<path:asset>')
@login_required
def api_score_history(asset):
    """Return score history for an asset (last 30 entries)."""
    try:
        asset = asset.upper()
        history = AssetScoreHistory.query.filter_by(asset=asset).order_by(AssetScoreHistory.recorded_at.asc()).limit(30).all()
        return jsonify({
            'asset': asset,
            'history': [{
                'date': h.recorded_at.strftime('%Y-%m-%d'),
                'score': h.score
            } for h in history]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/debug/save_scores')
@login_required
@admin_required
def debug_save_scores():
    """Manually save current scores for all assets (for testing)."""
    with app.app_context():
        for currency in MAJOR_CURRENCIES:
            try:
                score = get_asset_score(currency)
                save_asset_score_history(currency, score)
                print(f"Manual saved score for {currency}: {score}")
            except Exception as e:
                print(f"Error saving score for {currency}: {e}")
    return jsonify({'success': True, 'message': 'Scores saved for all assets'})

@app.route('/debug/check_history/<path:asset>')
@login_required
@admin_required
def debug_check_history(asset):
    asset = asset.upper()
    records = AssetScoreHistory.query.filter_by(asset=asset).order_by(AssetScoreHistory.recorded_at.desc()).all()
    return jsonify([{
        'id': r.id,
        'asset': r.asset,
        'score': r.score,
        'recorded_at': r.recorded_at.isoformat()
    } for r in records])

@app.route('/debug/list_history')
@login_required
@admin_required
def debug_list_history():
    assets = db.session.query(AssetScoreHistory.asset).distinct().all()
    return jsonify([a[0] for a in assets])

@app.route('/debug/manual_save')
@login_required
@admin_required
def debug_manual_save():
    """Force save current scores for all assets."""
    with app.app_context():
        for currency in MAJOR_CURRENCIES:
            try:
                score = get_asset_score(currency)
                save_asset_score_history(currency, score)
                print(f"Manual saved score for {currency}: {score}")
            except Exception as e:
                print(f"Error saving score for {currency}: {e}")
    return jsonify({'success': True, 'message': 'Manual save completed'})


@app.route('/api/currencies')
@login_required
def api_currencies():
    try:
        currencies = []
        for curr in MAJOR_CURRENCIES:
            econ_pct = analyze_currency_econ(curr)
            cot_rec = COTData.query.filter_by(currency=curr.upper()).first()
            cot_net = cot_rec.net_position if cot_rec else 0
            cot_change = cot_rec.weekly_change if cot_rec else 0
            longs = cot_rec.longs if cot_rec else 0
            shorts = cot_rec.shorts if cot_rec else 0
            total = longs + shorts
            long_pct = (longs / total * 100) if total > 0 else 0
            short_pct = (shorts / total * 100) if total > 0 else 0
            currencies.append({
                'currency': curr,
                'econ_pct': round(econ_pct, 1),
                'cot_net': cot_net,
                'cot_change': cot_change,
                'longs': longs,
                'shorts': shorts,
                'long_pct': long_pct,
                'short_pct': short_pct
            })
        return jsonify({'currencies': currencies})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/cot/history/<currency>')
@login_required
def api_cot_history(currency):
    """Return historical COT data for both admin table and user chart."""
    limit = request.args.get('limit', 50, type=int)
    history = get_historical_cot_with_changes(currency, limit)
    # Build separate arrays for the chart
    dates = [item['date'] for item in history]
    net_positions = [item['net'] for item in history]
    weekly_changes = [item['change_net'] if item['change_net'] is not None else 0 for item in history]
    # Get latest bias and trend
    latest = get_latest_cot(currency)
    bias_str = "Neutral"
    if latest:
        if latest['bias'] == 1:
            bias_str = "Bullish"
        elif latest['bias'] == -1:
            bias_str = "Bearish"
    trend = get_cot_trend(currency, 5)
    return jsonify({
        'currency': currency.upper(),
        'history': history,           # for admin table
        'dates': dates,               # for user chart
        'net_positions': net_positions,
        'weekly_changes': weekly_changes,
        'bias': bias_str,
        'trend': trend
    })

@app.route('/api/sentiment')
@login_required
def api_sentiment():
    data = SentimentData.query.all()
    return jsonify([{
        'pair': s.pair,
        'long_pct': s.long_pct,
        'short_pct': s.short_pct,
        'last_updated': s.last_updated.isoformat() if s.last_updated else None
    } for s in data])

@app.route('/api/retail-sentiment')
@login_required
def api_retail_sentiment_all():
    """Return latest retail sentiment for all pairs."""
    data = get_all_latest_retail_sentiment()
    return jsonify(data)

@app.route('/api/retail-sentiment/<pair>')
@login_required
def api_retail_sentiment_pair(pair):
    """Return latest retail sentiment for a specific pair."""
    pair = pair.upper().replace('/', '')
    data = get_latest_retail_sentiment(pair)
    if not data:
        return jsonify({'error': 'No data for this pair'}), 404
    return jsonify(data)

@app.route('/api/currency_strength')
@login_required
def api_currency_strength():
    """
    Returns currency strength scores based on economic sentiment and pair analysis.
    Each currency gets:
      - strength: overall score from -20 to +20 (based on pair analysis)
      - econ_pct: economic sentiment percentage (0-100) from analyze_currency_econ
    """
    try:
        # Compute economic sentiment percentage for each currency
        econ_data = {}
        for curr in MAJOR_CURRENCIES:
            econ_pct = analyze_currency_econ(curr)
            econ_data[curr] = econ_pct if econ_pct is not None else 50

        # Compute currency strength from pair analysis (if available, else use default)
        # Use cached analysis if present, otherwise run a minimal analysis
        global cached_analysis
        if cached_analysis is None or last_analysis_time is None or \
           (datetime.now() - last_analysis_time).total_seconds() > 300:
            # Build temporary results to calculate currency strength
            temp_results = []
            for base, quote in ALL_PAIRS:
                pair_name = f"{base}/{quote}"
                # Use simple scoring: sum of normalized indicator contributions (like in calculate_weighted_pair_score)
                # But to avoid heavy computation, we can reuse the existing calculate_weighted_pair_score
                weighted = calculate_weighted_pair_score(pair_name, base, quote)
                total_score = weighted["total_raw"]
                temp_results.append({'pair': pair_name, 'overall': {'score': total_score}})
            currency_ranking, currency_scores, _ = calculate_currency_strength(temp_results)
        else:
            # Use cached analysis from /api/analyze
            currency_ranking, currency_scores, _ = calculate_currency_strength(cached_analysis)

        # Prepare heatmap data
        heatmap_data = []
        for currency, score in currency_ranking:
            econ_pct = econ_data.get(currency, 50)
            # Score is already in range -20..20, ensure it's within limits
            score = max(-20, min(20, score))
            heatmap_data.append({
                'currency': currency,
                'strength': round(score, 1),
                'econ_pct': round(econ_pct, 1)
            })
        # Sort by strength descending
        heatmap_data.sort(key=lambda x: x['strength'], reverse=True)
        return jsonify({'currencies': heatmap_data})
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
#  ROUTES
# -----------------------------
@app.route('/admin')
@login_required
@admin_required
def admin_panel():
    return render_template('admin.html', username=session['username'], ALL_PAIRS=ALL_PAIRS, currencies=MAJOR_CURRENCIES)

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

# --- Economic Indicators Admin ---
@app.route('/admin/econ_indicators', methods=['GET'])
@login_required
@admin_required
def admin_econ_indicators():
    currency = request.args.get('currency')
    if not currency:
        return jsonify([])
    indicators = EconomicIndicator.query.filter_by(currency=currency).order_by(EconomicIndicator.indicator_name).all()
    return jsonify([{
        'id': i.id,
        'indicator_name': i.indicator_name,
        'forecast': i.forecast,
        'actual': i.actual,
        'is_lower_better': i.is_lower_better,
        'category': i.category or 'General',
        'last_updated': i.last_updated.isoformat() if i.last_updated else None
    } for i in indicators])

@app.route('/admin/econ_indicators', methods=['POST'])
@login_required
@admin_required
def admin_econ_indicator_create():
    data = request.json
    indicator = EconomicIndicator(
        currency=data['currency'],
        indicator_name=data['indicator_name'],
        forecast=float(data['forecast']),
        actual=float(data['actual']),
        is_lower_better=data.get('is_lower_better', False),
        category=data.get('category', 'General')
    )
    db.session.add(indicator)
    db.session.commit()
    return jsonify({'success': True})

@app.route('/admin/econ_indicators/<int:ind_id>', methods=['PUT', 'DELETE'])
@login_required
@admin_required
def admin_econ_indicator_modify(ind_id):
    ind = EconomicIndicator.query.get_or_404(ind_id)
    if request.method == 'DELETE':
        db.session.delete(ind)
        db.session.commit()
        return jsonify({'success': True})
    else:
        data = request.json
        ind.indicator_name = data.get('indicator_name', ind.indicator_name)
        ind.forecast = float(data.get('forecast', ind.forecast))
        ind.actual = float(data.get('actual', ind.actual))
        ind.is_lower_better = data.get('is_lower_better', ind.is_lower_better)
        ind.category = data.get('category', ind.category)
        db.session.commit()
        return jsonify({'success': True})

# --- COT Data Admin (with longs/shorts) ---
@app.route('/admin/cot_data', methods=['GET'])
@login_required
@admin_required
def admin_cot_data():
    cot_data = COTData.query.order_by(COTData.currency).all()
    return jsonify([{
        'id': c.id,
        'currency': c.currency,
        'net_position': c.net_position,
        'weekly_change': c.weekly_change,
        'longs': c.longs,
        'shorts': c.shorts,
        'last_updated': c.last_updated.isoformat() if c.last_updated else None
    } for c in cot_data])

@app.route('/admin/cot_history', methods=['POST'])
@login_required
@admin_required
def admin_cot_history_add():
    try:
        data = request.json
        currency = data['currency'].upper()
        report_date_str = data['report_date']
        longs = float(data['longs'])
        shorts = float(data['shorts'])
        change_longs = float(data['change_longs'])
        change_shorts = float(data['change_shorts'])

        net = longs - shorts
        weekly_change = change_longs - change_shorts

        from datetime import datetime
        report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()

        bias = 1 if net > 0 else (-1 if net < 0 else 0)

        new_record = COTHistory(
            currency=currency,
            report_date=report_date,
            long_positions=longs,
            short_positions=shorts,
            net_positions=net,
            weekly_change=weekly_change,
            bias=bias,
            change_longs=change_longs,      # store
            change_shorts=change_shorts     # store
        )
        db.session.add(new_record)
        db.session.commit()

        # Update current COTData entry
        current = COTData.query.filter_by(currency=currency).first()
        if not current:
            current = COTData(currency=currency)
            db.session.add(current)
        current.longs = longs
        current.shorts = shorts
        current.net_position = net
        current.weekly_change = weekly_change
        current.last_updated = datetime.utcnow()
        db.session.commit()

        return jsonify({'success': True, 'message': f'Record for {currency} added'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/admin/cot_history/<int:record_id>', methods=['DELETE'])
@login_required
@admin_required
def admin_cot_history_delete(record_id):
    try:
        record = COTHistory.query.get(record_id)
        if not record:
            return jsonify({'success': False, 'error': 'Record not found'}), 404
        currency = record.currency
        db.session.delete(record)
        db.session.commit()

        # Update current COTData to the latest remaining record
        latest = COTHistory.query.filter_by(currency=currency).order_by(COTHistory.report_date.desc()).first()
        current = COTData.query.filter_by(currency=currency).first()
        if latest and current:
            current.longs = latest.long_positions
            current.shorts = latest.short_positions
            current.net_position = latest.net_positions
            current.weekly_change = latest.weekly_change
            current.last_updated = datetime.utcnow()
            db.session.commit()
        elif not latest and current:
            current.longs = 0
            current.shorts = 0
            current.net_position = 0
            current.weekly_change = 0
            db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/admin/cot_data', methods=['POST'])
@login_required
@admin_required
def admin_cot_data_update():
    try:
        data = request.json
        currency = data['currency'].upper()
        report_date_str = data.get('report_date')
        
        # Validate numbers
        longs = float(data.get('longs', 0))
        shorts = float(data.get('shorts', 0))
        weekly_change = float(data.get('weekly_change', 0))
        
        cot = COTData.query.filter_by(currency=currency).first()
        if not cot:
            cot = COTData(currency=currency)
            db.session.add(cot)
        cot.longs = longs
        cot.shorts = shorts
        cot.net_position = longs - shorts
        cot.weekly_change = weekly_change
        db.session.commit()
        
        # Save historical snapshot
        from datetime import date
        if report_date_str:
            report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
        else:
            report_date = date.today()
        save_cot_snapshot(currency, report_date, longs, shorts, cot.net_position, weekly_change)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

# --- Add sync route here ---
@app.route('/admin/sync_cot_data')
@login_required
@admin_required
def sync_cot_data():
    currencies = ['USD','EUR','GBP','JPY','AUD','CAD','CHF','NZD','XAU','BTC']
    for cur in currencies:
        latest = COTHistory.query.filter_by(currency=cur).order_by(COTHistory.report_date.desc()).first()
        if latest:
            cot = COTData.query.filter_by(currency=cur).first()
            if not cot:
                cot = COTData(currency=cur)
                db.session.add(cot)
            cot.longs = latest.long_positions
            cot.shorts = latest.short_positions
            cot.net_position = latest.net_positions
            cot.weekly_change = latest.weekly_change
            cot.last_updated = datetime.utcnow()
    db.session.commit()
    return jsonify({'success': True, 'message': 'COTData synced from latest history'})

# --- Seasonality Admin ---
@app.route('/admin/seasonality/monthly/<pair>', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_monthly_bias(pair):
    if request.method == 'GET':
        biases = MonthlyBias.query.filter_by(pair=pair).all()
        data = {}
        for b in biases:
            data[b.month] = b.bias
        return jsonify(data)
    else:
        data = request.json
        MonthlyBias.query.filter_by(pair=pair).delete()
        for month_str, bias in data.items():
            month = int(month_str)
            if 1 <= month <= 12:
                db.session.add(MonthlyBias(pair=pair, month=month, bias=bias))
        db.session.commit()
        return jsonify({'success': True})

@app.route('/admin/seasonality/daterange/<pair>', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_date_ranges(pair):
    if request.method == 'GET':
        ranges = SeasonalityDateRange.query.filter_by(pair=pair).all()
        return jsonify([{
            'id': r.id,
            'start_month': r.start_month,
            'start_day': r.start_day,
            'end_month': r.end_month,
            'end_day': r.end_day,
            'bias': r.bias
        } for r in ranges])
    else:
        data = request.json
        dr = SeasonalityDateRange(
            pair=pair,
            start_month=int(data['start_month']),
            start_day=int(data['start_day']),
            end_month=int(data['end_month']),
            end_day=int(data['end_day']),
            bias=data['bias']
        )
        db.session.add(dr)
        db.session.commit()
        return jsonify({'success': True, 'id': dr.id})

@app.route('/admin/seasonality/daterange/<int:dr_id>', methods=['DELETE'])
@login_required
@admin_required
def admin_delete_date_range(dr_id):
    dr = SeasonalityDateRange.query.get_or_404(dr_id)
    db.session.delete(dr)
    db.session.commit()
    return jsonify({'success': True})

# --- Sentiment Admin ---
@app.route('/admin/sentiment', methods=['GET'])
@login_required
@admin_required
def admin_sentiment_list():
    data = SentimentData.query.all()
    return jsonify([{
        'id': s.id,
        'pair': s.pair,
        'long_pct': s.long_pct,
        'short_pct': s.short_pct,
        'last_updated': s.last_updated.isoformat() if s.last_updated else None
    } for s in data])

@app.route('/admin/sentiment', methods=['POST'])
@login_required
@admin_required
def admin_sentiment_create_or_update():
    data = request.json
    pair = data['pair']
    sent = SentimentData.query.filter_by(pair=pair).first()
    if not sent:
        sent = SentimentData(pair=pair)
        db.session.add(sent)
    sent.long_pct = float(data['long_pct'])
    sent.short_pct = float(data['short_pct'])
    db.session.commit()
    return jsonify({'success': True})

@app.route('/admin/sentiment/<int:sent_id>', methods=['DELETE'])
@login_required
@admin_required
def admin_sentiment_delete(sent_id):
    sent = SentimentData.query.get_or_404(sent_id)
    db.session.delete(sent)
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
        user = User(username=username, email=email, is_active=False)
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
    return render_template('dashboard.html', username=session['username'], is_admin=session.get('is_admin', False), favorites=user.get_favorites(), ALL_PAIRS=ALL_PAIRS)

@app.route('/currencies')
@login_required
def currencies():
    return render_template('currencies.html', username=session['username'], is_admin=session.get('is_admin', False))

@app.route('/scorecard')
@login_required
def scorecard():
    return render_template('scorecard.html', username=session['username'],
                           is_admin=session.get('is_admin', False),
                           currencies=MAJOR_CURRENCIES)

@app.route('/forex-scorecard')
@login_required
def forex_scorecard():
    return render_template(
        'forex_scorecard.html',
        username=session['username'],
        is_admin=session.get('is_admin', False),
        all_pairs=ALL_PAIRS,
        currencies=MAJOR_CURRENCIES
    )
@app.route('/test')
def test():
    return "Flask is working"

@app.route('/debug/routes')
def debug_routes():
    routes = [str(rule) for rule in app.url_map.iter_rules()]
    return jsonify(routes)
@app.route('/sentiment')
@login_required
def sentiment_page():
    return render_template('sentiment.html', username=session['username'], is_admin=session.get('is_admin', False))

@app.route('/heatmap')
@login_required
def heatmap():
    return render_template('heatmap.html', username=session['username'], is_admin=session.get('is_admin', False))

@app.route('/api/history')
@login_required
def api_history():
    histories = AnalysisHistory.query.filter_by(user_id=session['user_id']).order_by(AnalysisHistory.created_at.desc()).limit(50).all()
    return jsonify([{'date': h.created_at.strftime('%Y-%m-%d %H:%M:%S'), 'pair': h.pair, 'cot_bias': h.cot_bias, 'econ_bias': h.econ_bias, 'trend_score': h.trend_score, 'seasonality_bias': h.seasonality_bias, 'overall_bias': h.overall_bias} for h in histories])

@app.route('/profile')
@login_required
def profile():
    return render_template('profile.html', username=session['username'])

@app.route('/debug/weighted/<pair>')
@login_required
def debug_weighted(pair):
    try:
        base, quote = pair.split('/')
        weighted = calculate_weighted_pair_score(pair, base.upper(), quote.upper())
        return jsonify({
            'pair': pair,
            'total_raw': weighted['total_raw'],
            'breakdown': weighted['breakdown'],
            'category_scores': weighted['category_scores']
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# -----------------------------
# CREATE TEMPLATES (all complete)
# -----------------------------
def create_templates():
    os.makedirs('templates', exist_ok=True)

    # Login
    with open('templates/login.html', 'w') as f:
        f.write('''<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tradion · Login</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;height:100vh;display:flex;overflow:hidden}.split-container{display:flex;width:100%}.brand-panel{flex:1.2;background:radial-gradient(circle at 20% 30%, #121826 0%, #0B0F1A 100%);display:flex;flex-direction:column;justify-content:center;align-items:center;padding:3rem;position:relative;overflow:hidden}.brand-content{max-width:500px;z-index:2;text-align:center}.logo{font-size:3.8rem;font-weight:800;letter-spacing:4px;background:linear-gradient(135deg, #00e5ff 0%, #00b8d4 80%);-webkit-background-clip:text;background-clip:text;color:transparent;margin-bottom:1rem}.tagline{font-size:1.4rem;color:#a0b0c0;margin-bottom:3rem;font-weight:300}.chart-animation{width:100%;height:200px;margin-top:2rem;opacity:0.6}.chart-line{stroke:#00e5ff;stroke-width:2;fill:none;stroke-dasharray:1000;stroke-dashoffset:1000;animation:drawLine 4s ease-out forwards}@keyframes drawLine{to{stroke-dashoffset:0}}.glow-pulse{position:absolute;width:300px;height:300px;background:radial-gradient(circle, rgba(0,229,255,0.15) 0%, transparent 70%);border-radius:50%;top:20%;left:30%;animation:pulse 8s infinite alternate;z-index:1}@keyframes pulse{0%{transform:scale(1);opacity:0.3}100%{transform:scale(1.5);opacity:0.1}}.login-panel{flex:0.8;background:#0B0F1A;display:flex;align-items:center;justify-content:center;padding:2rem}.login-card{background:#121826;border-radius:20px;padding:2.5rem 2rem;width:100%;max-width:420px;box-shadow:0 20px 40px rgba(0,0,0,0.6),0 0 0 1px rgba(0,229,255,0.1);border:1px solid #2a3040}.login-card h2{font-size:2rem;font-weight:600;color:#FFFFFF;margin-bottom:0.5rem}.subtitle{color:#8892b0;margin-bottom:2rem;font-size:0.95rem}.input-group{margin-bottom:1.5rem}.input-group label{display:block;margin-bottom:0.5rem;color:#ccd6f6;font-size:0.9rem;font-weight:500}.input-wrapper{position:relative}.input-wrapper input{width:100%;padding:0.9rem 1rem;background:#1a1f2e;border:1.5px solid #2a3040;border-radius:12px;color:#fff;font-size:1rem;transition:all 0.2s}.input-wrapper input:focus{outline:none;border-color:#00e5ff;box-shadow:0 0 0 3px rgba(0,229,255,0.1)}.input-wrapper input::placeholder{color:#5a6380}.password-toggle{position:absolute;right:15px;top:50%;transform:translateY(-50%);color:#8892b0;cursor:pointer;font-size:1.2rem}.btn-login{width:100%;padding:0.9rem;background:linear-gradient(135deg, #00e5ff 0%, #00b8d4 100%);border:none;border-radius:12px;color:#0B0F1A;font-weight:700;font-size:1.1rem;cursor:pointer;transition:all 0.2s;margin-top:0.5rem}.btn-login:hover{transform:translateY(-2px);box-shadow:0 10px 20px rgba(0,229,255,0.2)}.btn-login:active{transform:translateY(0)}.error-message{background:rgba(255,77,109,0.15);color:#ff8099;padding:0.8rem;border-radius:10px;margin-bottom:1rem;font-size:0.9rem;border-left:4px solid #ff4d6d}.footer-text{text-align:center;margin-top:1.5rem;color:#8892b0;font-size:0.9rem}.footer-text a{color:#00e5ff;text-decoration:none;font-weight:600}.footer-text a:hover{text-decoration:underline}@media (max-width:768px){.split-container{flex-direction:column}.brand-panel{display:none}}</style>
</head>
<body>
<div class="split-container">
<div class="brand-panel"><div class="glow-pulse"></div><div class="brand-content"><div class="logo">Tradion</div><div class="tagline">Trade smarter. Analyze deeper.</div><svg class="chart-animation" viewBox="0 0 400 100" preserveAspectRatio="none"><polyline class="chart-line" points="0,80 50,60 100,70 150,20 200,40 250,10 300,50 350,30 400,45"/></svg></div></div>
<div class="login-panel"><div class="login-card"><h2>Welcome Back</h2><p class="subtitle">Access your trading dashboard</p>{% if error %}<div class="error-message">{{ error }}</div>{% endif %}<form method="POST"><div class="input-group"><label>Username</label><div class="input-wrapper"><input type="text" name="username" placeholder="Enter username" required autofocus></div></div><div class="input-group"><label>Password</label><div class="input-wrapper"><input type="password" name="password" id="password" placeholder="••••••••" required><span class="password-toggle" onclick="togglePassword()">👁</span></div></div><button type="submit" class="btn-login">Sign In</button></form><p class="footer-text">Don't have an account? <a href="{{ url_for('register') }}">Sign up</a></p></div></div>
</div>
<script>function togglePassword(){const pwd=document.getElementById('password');const toggle=document.querySelector('.password-toggle');if(pwd.type==='password'){pwd.type='text';toggle.textContent='🙈'}else{pwd.type='password';toggle.textContent='👁'}}</script>
</body>
</html>''')

    # Register
    with open('templates/register.html', 'w') as f:
        f.write('''<!DOCTYPE html>
<html><head><title>Register - Tradion</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI',sans-serif;background:#0B0F1A;min-height:100vh;display:flex;justify-content:center;align-items:center}.register-container{background:#121826;border-radius:20px;padding:40px;width:100%;max-width:400px;box-shadow:0 0 40px rgba(0,229,255,0.1);border:1px solid #2a3040}h2{color:#00e5ff;text-align:center;margin-bottom:30px}input{width:100%;padding:12px;margin:10px 0;border:none;border-radius:10px;background:#1a1f2e;color:#fff;border:1.5px solid #2a3040}input:focus{border-color:#00e5ff;outline:none}button{width:100%;padding:12px;background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;border:none;border-radius:10px;cursor:pointer;font-weight:bold}.error{color:#ff8099;text-align:center;margin-top:10px}.link{text-align:center;margin-top:20px}.link a{color:#00e5ff}</style>
</head>
<body>
<div class="register-container"><h2>Create Account</h2><form method="POST"><input type="text" name="username" placeholder="Username" required><input type="email" name="email" placeholder="Email" required><input type="password" name="password" placeholder="Password" required><input type="password" name="confirm_password" placeholder="Confirm Password" required><button type="submit">Register</button></form>{% if error %}<div class="error">{{ error }}</div>{% endif %}<div class="link"><a href="{{ url_for('login') }}">Back to Login</a></div></div>
</body>
</html>''')

                     # Dashboard (with Lucide icons and row background gradient matching score) - FIXED SEARCH
with open('templates/dashboard.html', 'w') as f:
    f.write('''<!DOCTYPE html>
<html><head><title>Tradion · Dashboard</title><meta name="viewport" content="width=device-width, initial-scale=1.0">
<script src="https://unpkg.com/lucide@latest"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;display:flex}.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100;transition:width 0.3s}.sidebar.collapsed{width:80px}.sidebar .logo{font-size:24px;font-weight:800;color:#00e5ff;text-align:center;margin-bottom:30px;padding-bottom:20px;border-bottom:1px solid #2a3040;display:flex;align-items:center;justify-content:center}.sidebar.collapsed .logo{font-size:20px}.sidebar.collapsed .logo span{display:none}.sidebar .menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:10px;cursor:pointer;transition:all 0.2s;color:#a0b0c0}.sidebar .menu-item:hover{background:rgba(0,229,255,0.08);color:#00e5ff}.sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;font-weight:bold}.sidebar .menu-icon{width:20px;height:20px;margin-right:12px;stroke-width:2}.sidebar.collapsed .menu-icon{margin-right:0}.sidebar.collapsed .menu-item span:not(.menu-icon){display:none}.main-content{flex:1;margin-left:280px;padding:20px 30px;transition:margin-left 0.3s}.navbar{background:#121826;padding:15px 25px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #2a3040;border-radius:0 0 16px 16px;margin-bottom:20px}.navbar-title{font-size:18px;color:#00e5ff}button{padding:10px 20px;background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;border:none;border-radius:8px;cursor:pointer;font-weight:bold;transition:all 0.2s}button:hover{transform:scale(1.02);box-shadow:0 0 15px rgba(0,229,255,0.3)}button.secondary{background:transparent;border:1px solid #00e5ff;color:#00e5ff}.search-input{padding:10px 15px;background:#1a1f2e;border:1px solid #2a3040;border-radius:8px;color:#fff;font-size:14px;width:200px;margin-left:10px}.search-input:focus{outline:none;border-color:#00e5ff}.table-container{overflow-x:auto;margin-top:20px;border-radius:12px;border:1px solid #2a3040;position:relative}table{width:100%;border-collapse:collapse;background:#121826;font-size:12px}th,td{padding:10px 8px;text-align:center;border-bottom:1px solid #2a3040;white-space:nowrap}th{background:rgba(0,229,255,0.1);color:#00e5ff;font-weight:600}tr:hover{background:rgba(0,229,255,0.05)}.score-positive{color:#00e5a0;font-weight:bold}.score-negative{color:#ff4d6d;font-weight:bold}.score-neutral{color:#ffb800}.content-pane{display:none}.content-pane.active{display:block}.hamburger{display:none;font-size:28px;cursor:pointer;color:#00e5ff;position:fixed;top:15px;left:20px;z-index:1100;background:#121826;padding:8px 12px;border-radius:8px;border:1px solid #2a3040}
.sidebar.collapsed{width:80px !important;min-width:80px !important}
.sidebar.collapsed ~ .main-content{margin-left:80px !important}
/* Conditional highlighting – same colors as Score column */
.indicator-positive {
    background-color: rgba(0, 229, 160, 0.15);
    color: #00e5a0;
    font-weight: 500;
}
.indicator-negative {
    background-color: rgba(255, 77, 109, 0.15);
    color: #ff4d6d;
    font-weight: 500;
}
/* Ensure cells without highlight keep normal styling */
td {
    background-color: inherit;
}
/* Sticky clone table */
.sticky-header-clone {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    z-index: 1000;
    display: none;
    background: #121826;
    border-collapse: collapse;
    width: auto;
    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
}
.sticky-header-clone th {
    background: rgba(0,229,255,0.1);
    color: #00e5ff;
    font-weight: 600;
    padding: 10px 8px;
    text-align: center;
    border-bottom: 1px solid #2a3040;
    white-space: nowrap;
}
@media (max-width:768px){
    .sidebar{transform:translateX(-100%);width:260px !important}
    .sidebar.open{transform:translateX(0)}
    .sidebar.collapsed{width:260px !important; min-width:260px !important}
    .sidebar.collapsed .logo-text{opacity:1;visibility:visible}
    .sidebar.collapsed .menu-item span:not(.menu-icon){display:inline-block;opacity:1}
    .sidebar.collapsed .menu-icon{margin-right:12px}
    .main-content{margin-left:0 !important;padding:60px 15px 20px 15px !important;width:100%}
    .sidebar.collapsed ~ .main-content{margin-left:0 !important}
    .hamburger{display:block}
    .navbar{flex-direction:column;align-items:flex-start;gap:10px;padding:10px 15px}
    th,td{padding:8px 4px;font-size:10px}
    .search-input{width:100%;margin:10px 0 0 0}
    .toolbar{flex-wrap:wrap}
}
</style>
</head>
<body>
<div class="hamburger" onclick="toggleSidebar()">☰</div>
<div class="sidebar" id="sidebar">
    <div class="logo">
        <span class="logo-text">⚡ Tradion</span>
        <button class="sidebar-toggle" id="sidebarToggleBtn" onclick="toggleSidebarCollapse()">◀</button>
    </div>
    <div class="menu-item active" onclick="showPane('analysis')"><i data-lucide="chart-line" class="menu-icon"></i><span>Analysis</span></div>
    <div class="menu-item" onclick="window.location.href='/currencies'"><i data-lucide="currency" class="menu-icon"></i><span>COT Data</span></div>
    <div class="menu-item" onclick="window.location.href='/scorecard'"><i data-lucide="trending-up" class="menu-icon"></i><span>Asset Scorecard</span></div>
<div class="menu-item" onclick="window.location.href='/forex-scorecard'"><i data-lucide="trending-up" class="menu-icon"></i><span>Forex Scorecard</span></div>
    <div class="menu-item" onclick="window.location.href='/sentiment'"><i data-lucide="message-circle" class="menu-icon"></i><span>Sentiment</span></div>
    <div class="menu-item" onclick="showPane('history')"><i data-lucide="clock" class="menu-icon"></i><span>History</span></div>
    {% if is_admin %}<div class="menu-item" onclick="window.location.href='/admin'"><i data-lucide="crown" class="menu-icon"></i><span>Admin</span></div>{% endif %}
    <div class="menu-item" onclick="window.location.href='/heatmap'"><i data-lucide="flame" class="menu-icon"></i><span>Heatmap</span></div>
    <div class="menu-item" onclick="window.location.href='/profile'"><i data-lucide="user" class="menu-icon"></i><span>Profile</span></div>
    <div class="menu-item" onclick="logout()"><i data-lucide="log-out" class="menu-icon"></i><span>Logout</span></div>
</div>
<div class="main-content" id="mainContent">
    <div class="navbar"><div class="navbar-title">Welcome, {{ username }}! {% if is_admin %}<span style="background:#ffb800;padding:4px 12px;border-radius:20px;font-size:12px;color:#000">👑 ADMIN</span>{% endif %}</div><div><span id="lastUpdateTime"></span></div></div>
    <div id="analysisPane" class="content-pane active">
        <div class="toolbar" style="margin-bottom:20px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
            <button onclick="loadDetailedAnalysis()">🔥 RUN ANALYSIS</button>
            <button onclick="refreshAnalysis()" class="secondary">🔄 Force Refresh</button>
            <input type="text" id="searchInput" class="search-input" placeholder="🔍 Search pair..." onkeyup="filterTable()">
        </div>
        <div id="loading" style="display:none"><div class="loading-skeleton">Loading...</div></div>
        <div class="table-container">
            <table id="detailedTable">
                <thead>
                    <tr>
                        <th>Symbol</th><th>Bias</th><th>Score</th>
                        <th colspan="2">Technical</th><th colspan="2">Sentiment</th>
                        <th colspan="5">Economic Growth & Consumer Strength</th>
                        <th colspan="3">Inflation</th><th>Interest Rates</th>
                        <th colspan="6">Job Market</th>
                    </tr>
                    <tr>
                        <th></th><th></th><th></th>
                        <th>Trend</th><th>Seasonality</th>
                        <th>COT</th><th>Crowd</th>
                        <th>GDP</th><th>M-PMI</th><th>S-PMI</th><th>Retail</th><th>Cons Conf</th>
                        <th>CPI</th><th>PPI</th><th>PCE</th><th>Interest</th>
                        <th>NFP</th><th>Avg Hrly</th><th>Unemp Rate</th><th>Unemp Claims</th><th>ADP</th><th>JOLTS</th>
                    </tr>
                </thead>
                <tbody id="detailedTableBody"><tr><td colspan="22">Click "RUN ANALYSIS" to load data</tbody>
            </table>
        </div>
    </div>
    <div id="historyPane" class="content-pane"><div id="historyContent" style="margin-top:20px"></div></div>
</div>
<script>
let currentDetailed = [];

// Cache keys
const CACHE_KEY = 'tradion_detailed_analysis';
const CACHE_TIME_KEY = 'tradion_detailed_analysis_time';
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// Helper to get cell class based on numeric value
function getIndicatorClass(value) {
    if (value > 0) return 'indicator-positive';
    if (value < 0) return 'indicator-negative';
    return '';
}

function updateSidebarToggleIcon() {
    const sidebar = document.getElementById('sidebar');
    const btn = document.getElementById('sidebarToggleBtn');
    if (!btn) return;
    if (sidebar.classList.contains('collapsed')) {
        btn.innerHTML = '▶'; // collapsed → show right arrow (expand)
    } else {
        btn.innerHTML = '◀'; // expanded → show left arrow (collapse)
    }
}

function toggleSidebar() {
    document.getElementById('sidebar').classList.toggle('open');
}
function toggleSidebarCollapse() {
    const sidebar = document.getElementById('sidebar');
    if (window.innerWidth > 768) {
        sidebar.classList.toggle('collapsed');
        localStorage.setItem('sidebarCollapsed', sidebar.classList.contains('collapsed'));
        updateSidebarToggleIcon();
    }
}
function restoreSidebarState() {
    const sidebar = document.getElementById('sidebar');
    if (window.innerWidth > 768) {
        const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
        if (isCollapsed) sidebar.classList.add('collapsed');
        else sidebar.classList.remove('collapsed');
        updateSidebarToggleIcon();
    }
}
window.addEventListener('resize', function() {
    const sidebar = document.getElementById('sidebar');
    if (window.innerWidth <= 768) {
        sidebar.classList.remove('collapsed');
        updateSidebarToggleIcon();
    } else {
        const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
        if (isCollapsed) sidebar.classList.add('collapsed');
        else sidebar.classList.remove('collapsed');
        updateSidebarToggleIcon();
    }
});
function showPane(pane) {
    document.querySelectorAll('.content-pane').forEach(p=>p.classList.remove('active'));
    document.getElementById(pane+'Pane').classList.add('active');
    if (pane === 'history') loadHistory();
}

async function loadDetailedAnalysis(forceRefresh = false) {
    if (!forceRefresh) {
        const cachedData = sessionStorage.getItem(CACHE_KEY);
        const cachedTime = sessionStorage.getItem(CACHE_TIME_KEY);
        if (cachedData && cachedTime && (Date.now() - parseInt(cachedTime)) < CACHE_TTL) {
            try {
                let data = JSON.parse(cachedData);
                data.sort((a, b) => b.score - a.score);
                currentDetailed = data;
                renderDetailedTable(data);
                document.getElementById('lastUpdateTime').innerHTML = 'Cached: ' + new Date(parseInt(cachedTime)).toLocaleTimeString();
                return;
            } catch(e) { console.warn('Cache parse error', e); }
        }
    }

    document.getElementById('loading').style.display = 'block';
    try {
        const res = await fetch('/api/detailed_analysis');
        if (!res.ok) throw new Error('HTTP ' + res.status);
        let data = await res.json();
        data.sort((a, b) => b.score - a.score);
        currentDetailed = data;
        renderDetailedTable(data);
        const now = Date.now();
        sessionStorage.setItem(CACHE_KEY, JSON.stringify(data));
        sessionStorage.setItem(CACHE_TIME_KEY, now.toString());
        document.getElementById('lastUpdateTime').innerHTML = 'Updated: ' + new Date().toLocaleTimeString();
    } catch(e) {
        console.error('Analysis error:', e);
        document.getElementById('detailedTableBody').innerHTML = `<tr><td colspan="22" style="color:#ff4d6d">❌ Failed to load analysis. Please try again.  </td></tr>`;
    } finally {
        document.getElementById('loading').style.display = 'none';
    }
}

function renderDetailedTable(data) {
    const tbody = document.getElementById('detailedTableBody');
    tbody.innerHTML = '';
    data.forEach(item => {
        const row = tbody.insertRow();
        // Symbol cell (no highlight)
        row.insertCell(0).innerText = item.symbol;
        // Bias cell (text only)
        row.insertCell(1).innerHTML = `<strong>${item.bias}</strong>`;
        // Score cell (already has its own styling)
        const scoreCell = row.insertCell(2);
        const score = item.score;
        let bgColor = '';
        if (score >= 8) bgColor = 'rgba(0, 229, 160, 0.5)';
        else if (score >= 6) bgColor = 'rgba(0, 229, 160, 0.4)';
        else if (score >= 4) bgColor = 'rgba(0, 229, 160, 0.3)';
        else if (score >= 2) bgColor = 'rgba(0, 229, 160, 0.2)';
        else if (score > 0)  bgColor = 'rgba(0, 229, 160, 0.1)';
        else if (score <= -8) bgColor = 'rgba(255, 77, 109, 0.5)';
        else if (score <= -6) bgColor = 'rgba(255, 77, 109, 0.4)';
        else if (score <= -4) bgColor = 'rgba(255, 77, 109, 0.3)';
        else if (score <= -2) bgColor = 'rgba(255, 77, 109, 0.2)';
        else if (score < 0)  bgColor = 'rgba(255, 77, 109, 0.1)';
        scoreCell.innerHTML = `<span class="${score > 0 ? 'score-positive' : (score < 0 ? 'score-negative' : 'score-neutral')}">${score}</span>`;
        if (bgColor) scoreCell.style.backgroundColor = bgColor;

        // Indicator columns (all numeric)
        const trend = item.trend;
        const seasonality = item.seasonality;
        const cot = item.cot;
        const crowd = item.crowd;
        const gdp = item.gdp;
        const mPmi = item.m_pmi;
        const sPmi = item.s_pmi;
        const retail = item.retail;
        const consConf = item.consumer_conf;
        const cpi = item.cpi;
        const ppi = item.ppi;
        const pce = item.pce;
        const interest = item.interest;
        const nfp = item.nfp;
        const avgHrly = item.avg_hourly_earnings;
        const unempRate = item.unemp_rate;
        const unempClaims = item.unemp_claims;
        const adp = item.adp;
        const jolts = item.jolts;

        // Helper to create a cell with conditional class
        const addCell = (value) => {
            const cell = row.insertCell();
            cell.innerText = value;
            const cls = getIndicatorClass(value);
            if (cls) cell.classList.add(cls);
        };

        addCell(trend);
        addCell(seasonality);
        addCell(cot);
        addCell(crowd);
        addCell(gdp);
        addCell(mPmi);
        addCell(sPmi);
        addCell(retail);
        addCell(consConf);
        addCell(cpi);
        addCell(ppi);
        addCell(pce);
        addCell(interest);
        addCell(nfp);
        addCell(avgHrly);
        addCell(unempRate);
        addCell(unempClaims);
        addCell(adp);
        addCell(jolts);
    });
    // Remove any existing "no results" message
    const existingMsg = document.getElementById('noResultsMsg');
    if (existingMsg) existingMsg.remove();
    // Re-apply search filter after loading new data
    filterTable();
    initStickyHeader();
}

// ========== FIXED SEARCH FUNCTION ==========
function filterTable() {
    const input = document.getElementById('searchInput');
    if (!input) return;
    const filter = input.value.trim().toLowerCase();
    const table = document.getElementById('detailedTable');
    const tbody = table.querySelector('tbody');
    if (!tbody) return;
    const rows = tbody.querySelectorAll('tr');
    let hasVisible = false;
    
    rows.forEach(row => {
        const symbolCell = row.cells[0];
        if (symbolCell) {
            const symbol = (symbolCell.textContent || symbolCell.innerText).toLowerCase();
            const matches = filter === '' || symbol.indexOf(filter) > -1;
            row.style.display = matches ? '' : 'none';
            if (matches) hasVisible = true;
        }
    });
    
    // Show "no results" message if needed
    let noResultMsg = document.getElementById('noResultsMsg');
    if (!hasVisible && filter !== '') {
        if (!noResultMsg) {
            noResultMsg = document.createElement('div');
            noResultMsg.id = 'noResultsMsg';
            noResultMsg.style.textAlign = 'center';
            noResultMsg.style.padding = '20px';
            noResultMsg.style.color = '#ffb800';
            tbody.parentNode.insertBefore(noResultMsg, tbody.nextSibling);
        }
        noResultMsg.innerText = `🔍 No pairs matching "${filter}"`;
        noResultMsg.style.display = 'block';
    } else if (noResultMsg) {
        noResultMsg.style.display = 'none';
    }
}
// ==========================================

async function refreshAnalysis() {
    sessionStorage.removeItem(CACHE_KEY);
    sessionStorage.removeItem(CACHE_TIME_KEY);
    await loadDetailedAnalysis(true);
}

async function loadHistory() {
    const r = await fetch('/api/history');
    const d = await r.json();
    let html = '<td><thead><tr><th>Date</th><th>Pair</th><th>COT</th><th>Economic</th><th>Trend</th><th>Seasonality</th><th>Overall</th></tr></thead><tbody>';
    d.forEach(i=>{html+=`<tr><td>${i.date}</td><td>${i.pair}</td><td>${i.cot_bias}</td><td>${i.econ_bias}</td><td>${i.trend_score}</td><td>${i.seasonality_bias||'N/A'}</td><td>${i.overall_bias}</td>`});
    html+='</tbody><table>';
    document.getElementById('historyContent').innerHTML = html;
}

function logout() { fetch('/logout').then(()=>window.location.href='/login'); }

// Sticky header (same as before, unchanged)
let stickyHeader = null;

function initStickyHeader() {
    if (stickyHeader && stickyHeader.parentNode) stickyHeader.parentNode.removeChild(stickyHeader);
    const originalTable = document.getElementById('detailedTable');
    if (!originalTable) return;
    const cloneTable = originalTable.cloneNode(true);
    const tbody = cloneTable.querySelector('tbody');
    if (tbody) tbody.remove();
    cloneTable.style.position = 'fixed';
    cloneTable.style.top = '0';
    cloneTable.style.left = '0';
    cloneTable.style.zIndex = '1000';
    cloneTable.style.display = 'none';
    cloneTable.style.background = '#121826';
    cloneTable.style.borderCollapse = 'collapse';
    cloneTable.style.width = originalTable.offsetWidth + 'px';
    cloneTable.classList.add('sticky-header-clone');
    originalTable.parentNode.insertBefore(cloneTable, originalTable);
    stickyHeader = cloneTable;
    syncColumnWidths(originalTable, cloneTable);
    function handleScroll() {
        if (!originalTable) return;
        const rect = originalTable.getBoundingClientRect();
        const shouldSticky = rect.top <= 0;
        if (shouldSticky && stickyHeader.style.display !== 'table') {
            stickyHeader.style.display = 'table';
            syncColumnWidths(originalTable, stickyHeader);
            stickyHeader.style.left = originalTable.parentNode.getBoundingClientRect().left + 'px';
            stickyHeader.style.width = originalTable.offsetWidth + 'px';
        } else if (!shouldSticky && stickyHeader.style.display === 'table') {
            stickyHeader.style.display = 'none';
        }
    }
    window.addEventListener('scroll', handleScroll);
    window.addEventListener('resize', function() {
        if (stickyHeader.style.display === 'table') {
            syncColumnWidths(originalTable, stickyHeader);
            stickyHeader.style.left = originalTable.parentNode.getBoundingClientRect().left + 'px';
            stickyHeader.style.width = originalTable.offsetWidth + 'px';
        }
    });
    handleScroll();
}

function syncColumnWidths(sourceTable, targetTable) {
    const sourceRows = sourceTable.querySelectorAll('thead tr');
    const targetRows = targetTable.querySelectorAll('thead tr');
    for (let r = 0; r < sourceRows.length && r < targetRows.length; r++) {
        const sourceCells = sourceRows[r].cells;
        const targetCells = targetRows[r].cells;
        for (let c = 0; c < sourceCells.length && c < targetCells.length; c++) {
            targetCells[c].style.width = sourceCells[c].offsetWidth + 'px';
        }
    }
}

restoreSidebarState();
loadDetailedAnalysis();

setTimeout(() => {
    if (typeof lucide !== 'undefined') lucide.createIcons();
}, 100);
</script>
</body>
</html>''')

#Asset scorecard template
    # Scorecard template (with Lucide icons) - Bond indicator fixed
    with open('templates/scorecard.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Asset Scorecard – Tradion</title>
    <script src="https://unpkg.com/lucide@latest"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;display:flex}
        .sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100}
        .sidebar .logo{font-size:24px;font-weight:800;color:#00e5ff;text-align:center;margin-bottom:30px;padding-bottom:20px;border-bottom:1px solid #2a3040}
        .sidebar .menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:10px;cursor:pointer;transition:all 0.2s;color:#a0b0c0}
        .sidebar .menu-item:hover{background:rgba(0,229,255,0.08);color:#00e5ff}
        .sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;font-weight:bold}
        .sidebar .menu-icon{font-size:20px;margin-right:12px}
        .sidebar .menu-icon svg{width:20px;height:20px;vertical-align:middle}
        .main-content{flex:1;margin-left:280px;padding:12px 20px}  /* reduced top/bottom padding */
        .header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}  /* reduced margin */
        .header h2{color:#00e5ff;font-size:1.8rem}
        .symbol-selector select{padding:8px 12px;background:#121826;border:1.5px solid #2a3040;color:#fff;border-radius:8px;font-size:0.9rem;min-width:180px}
        .scorecard-grid{display:grid;grid-template-columns:0.9fr 1.1fr;gap:12px}  /* reduced gap */
        .gauge-panel{
            background:#121826;
            border-radius:16px;
            padding:12px 12px 16px 12px;  /* reduced padding */
            display:flex;
            flex-direction:column;
            align-items:center;
            border:1px solid #2a3040;
            position:relative;
        }
        .gauge-container{
            position:relative;
            width:160px;   /* slightly smaller */
            height:90px;
            margin-bottom:8px;
        }
        .gauge-svg{width:100%;height:100%}
        .gauge-bg{stroke:#2a3040;stroke-width:12;fill:none}
        .gauge-fill{stroke-width:12;fill:none;stroke-linecap:round;transition:stroke-dasharray 0.5s}
        .gauge-needle-group{transition:transform 0.5s ease-out}
        .needle-body{stroke:#fff;stroke-width:2;fill:none}
        .needle-tip{fill:#fff;stroke:#fff;stroke-width:1}
        .gauge-center{
            position:relative;
            bottom:auto;
            left:auto;
            transform:none;
            text-align:center;
            margin-top:4px;
        }
        .gauge-label{font-size:1.1rem;font-weight:bold;color:#00e5ff}
        .gauge-bias{font-size:0.85rem;color:#00e5a0}
        .loading-overlay{position:absolute;top:0;left:0;right:0;bottom:0;background:rgba(18,24,38,0.8);display:flex;align-items:center;justify-content:center;border-radius:16px;z-index:10}
        .loading-overlay.hidden{display:none}
        .spinner{border:3px solid #2a3040;border-top:3px solid #00e5ff;border-radius:50%;width:30px;height:30px;animation:spin 1s linear infinite}
        @keyframes spin{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}
        .score-summary{display:flex;flex-direction:column;gap:6px;width:100%;margin-top:8px}  /* reduced gap */
        .score-item{display:flex;justify-content:space-between;align-items:center;padding:4px 10px;background:#1a1f2e;border-radius:8px}
        .score-label{font-weight:500;font-size:0.85rem}
        .score-value{font-weight:bold;padding:2px 8px;border-radius:20px;font-size:0.75rem}
        .score-value.positive{background:rgba(0,229,160,0.15);color:#00e5a0}
        .score-value.negative{background:rgba(255,77,109,0.15);color:#ff4d6d}
        .score-value.neutral{background:rgba(255,184,0,0.15);color:#ffb800}
        /* Right column cards */
        .right-col{display:flex;flex-direction:column;gap:10px}  /* reduced gap */
        .two-cols{display:grid;grid-template-columns:1fr 1fr;gap:10px}
        .panel{
            background:#121826;
            border-radius:12px;
            padding:10px;  /* reduced padding */
            border:1px solid #2a3040;
        }
        .panel h3{
            color:#00e5ff;
            font-size:0.9rem;
            margin-bottom:6px;
            border-bottom:1px solid #2a3040;
            padding-bottom:4px;
        }
        .indicator-row{
            display:flex;
            justify-content:space-between;
            padding:3px 0;
            border-bottom:1px solid rgba(42,48,64,0.3);
        }
        .indicator-row:last-child{border-bottom:none}
        .indicator-label{font-size:0.75rem}
        .indicator-values{display:flex;gap:8px;align-items:center}
        .value{font-weight:500;width:45px;text-align:right;font-size:0.75rem}
        .value.positive{color:#00e5a0}
        .value.negative{color:#ff4d6d}
        .value.neutral{color:#a0b0c0}
        .surprise{font-size:0.7rem;padding:1px 4px;border-radius:4px}
        .surprise.positive{background:rgba(0,229,160,0.15);color:#00e5a0}
        .surprise.negative{background:rgba(255,77,109,0.15);color:#ff4d6d}
        /* Table styles */
        .panel table {
            width: 100%;
            border-collapse: collapse;
        }
        .panel th, .panel td {
            padding: 4px 3px;
            text-align: left;
            font-size: 0.7rem;
        }
        .panel th:nth-child(1), .panel td:nth-child(1) { width: 40%; }
        .panel th:nth-child(2), .panel td:nth-child(2) { width: 15%; }
        .panel th:nth-child(3), .panel td:nth-child(3) { width: 15%; }
        .panel th:nth-child(4), .panel td:nth-child(4) { width: 15%; }
        .panel th:nth-child(5), .panel td:nth-child(5) { width: 15%; }
        .panel.crowd-table th:nth-child(1), .panel.crowd-table td:nth-child(1) { width: 50%; }
        .panel.crowd-table th:nth-child(2), .panel.crowd-table td:nth-child(2) { width: 50%; }
        /* Score History Chart */
        .history-chart-container {
            background: #121826;
            border-radius: 12px;
            padding: 10px;
            margin-top: 10px;
            border: 1px solid #2a3040;
        }
        .history-chart-container h3 {
            color: #00e5ff;
            font-size: 0.9rem;
            margin-bottom: 8px;
            border-bottom: 1px solid #2a3040;
            padding-bottom: 4px;
        }
        .chart-wrapper {
            position: relative;
            height: 240px;  /* reduced height to fit without scroll */
            width: 100%;
        }
        @media (max-width:768px){
            .main-content{margin-left:0;padding:60px 15px 20px;}
            .scorecard-grid{grid-template-columns:1fr;gap:15px}
            .gauge-container{width:140px;height:80px}
            .panel{padding:8px}
            .indicator-values{gap:6px}
            .value{width:40px}
            .chart-wrapper{height: 200px;}
            .two-cols{grid-template-columns:1fr;}
        }
    </style>
</head>
<body>
<div class="sidebar">
    <div class="logo">⚡ Tradion</div>
    <div class="menu-item" onclick="window.location.href='/dashboard'"><i data-lucide="chart-line" class="menu-icon"></i><span>Dashboard</span></div>
    <div class="menu-item" onclick="window.location.href='/currencies'"><i data-lucide="currency" class="menu-icon"></i><span>COT Data</span></div>
    <div class="menu-item active" onclick="window.location.href='/scorecard'"><i data-lucide="trending-up" class="menu-icon"></i><span>Asset Scorecard</span></div>
    <div class="menu-item" onclick="window.location.href='/forex-scorecard'"><i data-lucide="trending-up" class="menu-icon"></i><span>Forex Scorecard</span></div>
    <div class="menu-item" onclick="window.location.href='/profile'"><i data-lucide="user" class="menu-icon"></i><span>Profile</span></div>
    <div class="menu-item" onclick="logout()"><i data-lucide="log-out" class="menu-icon"></i><span>Logout</span></div>
</div>

<div class="main-content">
    <div class="header">
        <h2>Asset Scorecard</h2>
        <div class="symbol-selector">
            <select id="symbolSelect" onchange="loadScorecard()">
                <option value="">Select Currency...</option>
            </select>
        </div>
    </div>

    <div class="scorecard-grid">
        <div>
            <div class="gauge-panel" id="gaugePanel">
                <div class="loading-overlay" id="loadingOverlay">
                    <div class="spinner"></div>
                </div>
                <div class="gauge-container">
                    <svg class="gauge-svg" viewBox="0 0 220 110">
                        <defs>
                            <linearGradient id="gaugeGradient" x1="0%" y1="0%" x2="100%" y2="0%">
                                <stop offset="0%" stop-color="#ff4d6d"/>
                                <stop offset="50%" stop-color="#ffb800"/>
                                <stop offset="100%" stop-color="#00e5a0"/>
                            </linearGradient>
                            <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
                                <feDropShadow dx="1" dy="1" stdDeviation="1" flood-color="#000" flood-opacity="0.5"/>
                            </filter>
                        </defs>
                        <path class="gauge-bg" d="M20,110 A90,90 0 0,1 200,110"/>
                        <path id="gaugeFill" class="gauge-fill" d="M20,110 A90,90 0 0,1 200,110" stroke="url(#gaugeGradient)"/>
                        <g id="gaugeNeedleGroup" class="gauge-needle-group" transform="rotate(-90,110,110)" filter="url(#shadow)">
                            <line class="needle-body" x1="110" y1="110" x2="110" y2="30"/>
                            <circle class="needle-tip" cx="110" cy="30" r="4"/>
                        </g>
                    </svg>
                </div>
                <div class="gauge-center">
                    <div class="gauge-label" id="gaugeBias">Neutral</div>
                    <div class="gauge-bias" id="gaugeValue">0.0</div>
                </div>
                <div class="score-summary">
                    <div class="score-item"><span class="score-label">Tradion Score</span><span id="tradionScore" class="score-value neutral">0</span></div>
                    <div class="score-item"><span class="score-label">Technical</span><span id="technicalScore" class="score-value neutral">● 0</span></div>
                    <div class="score-item"><span class="score-label">COT Alignment</span><span id="sentimentCOTScore" class="score-value neutral">0</span></div>
                    <div class="score-item"><span class="score-label">Fundamentals</span><span id="fundamentalsScore" class="score-value neutral">0</span></div>
                </div>
            </div>

            <!-- Score History Chart -->
            <div class="history-chart-container">
                <h3>Tradion Score Overtime</h3>
                <div class="chart-wrapper">
                    <canvas id="scoreHistoryChart"></canvas>
                </div>
            </div>
        </div>
        <div class="right-col" id="categoryCards"></div>
    </div>
</div>

<script>
    const currencies = {{ currencies|tojson }};
    const select = document.getElementById('symbolSelect');
    currencies.forEach(cur => {
        const opt = document.createElement('option');
        opt.value = cur;
        opt.textContent = cur;
        select.appendChild(opt);
    });

    // Order of categories on the right side (as per screenshot)
    const CATEGORY_ORDER = [
        "Crowd Sentiment (COT)",
        "Technical Bias",
        "Economic Growth Bias",
        "Inflation Bias",
        "Jobs Market Bias"
    ];

    let historyChart = null;

    async function loadScorecard() {
        const symbol = select.value;
        if (!symbol) return;
        const overlay = document.getElementById('loadingOverlay');
        overlay.classList.remove('hidden');
        try {
            const res = await fetch('/api/asset_scorecard/' + encodeURIComponent(symbol));
            const data = await res.json();
            console.log('API response received. COT Alignment:', data.base_indicators.find(i => i.name === 'COT Alignment'));
            updateGauge(data.overall.score, data.overall.bias, data.overall.color);
            updateScores(data);
            renderCategoryCards(data);
            setTimeout(() => loadScoreHistory(symbol), 100);
        } catch(e) {
            console.error(e);
        } finally {
            overlay.classList.add('hidden');
        }
    }

    async function loadScoreHistory(asset) {
        try {
            const res = await fetch('/api/score_history/' + encodeURIComponent(asset));
            const data = await res.json();
            renderHistoryChart(data.history);
        } catch(e) {
            console.error('Error loading score history:', e);
        }
    }

    function renderHistoryChart(history) {
        const canvas = document.getElementById('scoreHistoryChart');
        const ctx = canvas.getContext('2d');
        if (historyChart) historyChart.destroy();

        if (!history || history.length === 0) {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.font = "12px 'Segoe UI'";
            ctx.fillStyle = "#8892b0";
            ctx.textAlign = "center";
            ctx.fillText("No score history yet. Data will appear after the first scheduled save (every 3 days).", canvas.width/2, canvas.height/2);
            return;
        }

        const labels = history.map(h => h.date);
        const scores = history.map(h => h.score);

        historyChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Score',
                    data: scores,
                    backgroundColor: scores.map(s => s > 0 ? 'rgba(0, 229, 160, 0.7)' : (s < 0 ? 'rgba(255, 77, 109, 0.7)' : 'rgba(160, 160, 160, 0.5)')),
                    borderColor: scores.map(s => s > 0 ? '#00e5a0' : (s < 0 ? '#ff4d6d' : '#a0a0a0')),
                    borderWidth: 1,
                    borderRadius: 4,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: (ctx) => `Score: ${ctx.raw.toFixed(1)}` } }
                },
                scales: {
                    y: {
                        title: { display: true, text: 'Score', color: '#a0b0c0' },
                        ticks: { color: '#e0e0e0' },
                        grid: {
                            color: (context) => context.tick.value === 0 ? '#2a3040' : 'transparent',
                            lineWidth: (context) => context.tick.value === 0 ? 1 : 0
                        },
                        min: -20,
                        max: 20
                    },
                    x: {
                        title: { display: true, text: 'Date', color: '#a0b0c0' },
                        ticks: { color: '#e0e0e0', maxRotation: 45, autoSkip: true },
                        grid: { display: false }
                    }
                }
            }
        });
    }

    function updateGauge(score, bias, color) {
        const clampedScore = Math.min(Math.max(score, -10), 10);
        const angle = ((clampedScore + 10) / 20) * 180;
        document.getElementById('gaugeNeedleGroup').setAttribute('transform', `rotate(${angle - 90}, 110, 110)`);
        document.getElementById('gaugeBias').textContent = bias;
        document.getElementById('gaugeValue').textContent = score.toFixed(1);
        document.querySelector('.gauge-bias').style.color = color;
    }

    function updateScores(data) {
        setScoreValue('tradionScore', data.tradion_score);
        setScoreValue('technicalScore', data.technical_score > 0 ? '▲ 1' : (data.technical_score < 0 ? '▼ -1' : '● 0'));
        setScoreValue('sentimentCOTScore', data.sentiment_cot_score);
        setScoreValue('fundamentalsScore', data.fundamentals_score,
                      data.fundamentals_score > 0 ? 'positive' : (data.fundamentals_score < 0 ? 'negative' : 'neutral'));
    }

    function setScoreValue(id, text, classNameOverride) {
        const el = document.getElementById(id);
        el.textContent = text;
        if (classNameOverride) {
            el.className = 'score-value ' + classNameOverride;
        } else {
            const val = parseFloat(text);
            el.className = 'score-value ' + (val > 0 ? 'positive' : val < 0 ? 'negative' : 'neutral');
        }
    }

    // Helper: map numeric bias score to label and CSS class
    function getBiasLabelAndClass(score) {
        if (score >= 5) return { label: 'Bullish', className: 'positive' };
        if (score <= -5) return { label: 'Bearish', className: 'negative' };
        return { label: 'Neutral', className: 'neutral' };
    }

    // Build an HTML string for a standard table with 5 columns (Indicator, Signal, Actual, Forecast, Surprise)
    function buildFullTable(indicators, data) {
        let html = `<div style="overflow-x:auto;">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th>Indicator</th>
                                    <th>Signal</th>
                                    <th>Actual</th>
                                    <th>Forecast</th>
                                    <th>Surprise</th>
                                </tr>
                            </thead>
                            <tbody>`;
        indicators.forEach(ind => {
            if (ind.name === '2-Year Bond Yield Trend') {
                const score = (ind.score !== undefined && ind.score !== null) ? Number(ind.score) : 0;
                let signal = score > 0 ? 'Bullish' : (score < 0 ? 'Bearish' : 'Neutral');
                let signalClass = score > 0 ? 'positive' : (score < 0 ? 'negative' : 'neutral');
                html += `<tr>
                            <td>${ind.name}</td>
                            <td class="value ${signalClass}">${signal}</td>
                            <td class="value">-</td><td class="value">-</td><td class="value">-</td>
                        </tr>`;
            }
            else if (ind.name === 'COT Alignment') {
                let score = (ind.score !== undefined && ind.score !== null) ? Number(ind.score) : 0;
                if (isNaN(score)) score = 0;
                let label = 'Neutral', labelClass = 'neutral';
                if (score > 0) { label = 'Bullish'; labelClass = 'positive'; }
                else if (score < 0) { label = 'Bearish'; labelClass = 'negative'; }
                html += `<tr>
                            <td>${ind.name}</td>
                            <td class="value ${labelClass}">${label}</td>
                            <td class="value">-</td><td class="value">-</td><td class="value">-</td>
                        </tr>`;
            }
            else {
                const surprise = ind.actual - ind.forecast;
                const better = ind.is_lower_better ? surprise < 0 : surprise > 0;
                const colorClass = better ? 'positive' : (surprise === 0 ? 'neutral' : 'negative');
                let signal = better ? 'Bullish' : (surprise === 0 ? 'Neutral' : 'Bearish');
                let signalClass = better ? 'positive' : (surprise === 0 ? 'neutral' : 'negative');
                html += `<tr>
                            <td>${ind.name}</td>
                            <td class="value ${signalClass}">${signal}</td>
                            <td class="value">${ind.actual}</td>
                            <td class="value">${ind.forecast}</td>
                            <td class="surprise ${colorClass}">${surprise > 0 ? '+' : ''}${surprise.toFixed(1)}</td>
                        </tr>`;
            }
        });
        html += `</tbody></table></div>`;
        return html;
    }

    // For Crowd Sentiment (COT) card we only need two columns (Indicator, Signal) and only the COT Alignment row
    function buildCrowdTable(data) {
        const cotInd = data.base_indicators.find(i => i.name === 'COT Alignment');
        if (!cotInd) return '<p>No data</p>';
        let score = (cotInd.score !== undefined && cotInd.score !== null) ? Number(cotInd.score) : 0;
        if (isNaN(score)) score = 0;
        let label = 'Neutral', labelClass = 'neutral';
        if (score > 0) { label = 'Bullish'; labelClass = 'positive'; }
        else if (score < 0) { label = 'Bearish'; labelClass = 'negative'; }
        return `<div style="overflow-x:auto;">
                    <table class="crowd-table">
                        <thead><tr><th>Indicator</th><th>Signal</th></tr></thead>
                        <tbody>
                            <tr><td>COT Alignment</td><td class="value ${labelClass}">${label}</td></tr>
                        </tbody>
                    </table>
                </div>`;
    }

    function renderCategoryCards(data) {
        const container = document.getElementById('categoryCards');
        container.innerHTML = '';

        // Separate the first two cards (Crowd Sentiment and Technical Bias) to be side by side
        const firstTwo = CATEGORY_ORDER.slice(0,2);   // ["Crowd Sentiment (COT)", "Technical Bias"]
        const rest = CATEGORY_ORDER.slice(2);

        // Create a wrapper for the two columns
        const twoColsDiv = document.createElement('div');
        twoColsDiv.className = 'two-cols';

        // Crowd Sentiment (COT) card – simplified table
        const crowdCategory = firstTwo[0];
        const crowdIndicators = data.base_indicators.filter(ind => ind.category === crowdCategory);
        const crowdBiasScore = data.category_bias[crowdCategory] || 0;
        const crowdBiasObj = getBiasLabelAndClass(crowdBiasScore);
        const crowdCard = document.createElement('div');
        crowdCard.className = 'panel';
        let crowdHtml = `<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                            <h3 style="margin:0;">${crowdCategory}</h3>
                            <span class="value ${crowdBiasObj.class}" style="background:rgba(255,255,255,0.05); padding:2px 8px; border-radius:20px; font-size:0.7rem;">${crowdBiasObj.label}</span>
                        </div>
                        <div class="indicator-row" style="font-weight:bold; margin-bottom:2px">
                            <span>Bias Score</span>
                            <span class="value ${crowdBiasScore > 0 ? 'positive' : crowdBiasScore < 0 ? 'negative' : 'neutral'}">${crowdBiasScore > 0 ? '+' : ''}${crowdBiasScore}</span>
                        </div>`;
        crowdHtml += buildCrowdTable(data);
        crowdCard.innerHTML = crowdHtml;
        twoColsDiv.appendChild(crowdCard);

        // Technical Bias card – full table
        const techCategory = firstTwo[1];
        const techIndicators = data.base_indicators.filter(ind => ind.category === techCategory);
        const techBiasScore = data.category_bias[techCategory] || 0;
        const techBiasObj = getBiasLabelAndClass(techBiasScore);
        const techCard = document.createElement('div');
        techCard.className = 'panel';
        let techHtml = `<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                            <h3 style="margin:0;">${techCategory}</h3>
                            <span class="value ${techBiasObj.class}" style="background:rgba(255,255,255,0.05); padding:2px 8px; border-radius:20px; font-size:0.7rem;">${techBiasObj.label}</span>
                        </div>
                        <div class="indicator-row" style="font-weight:bold; margin-bottom:2px">
                            <span>Bias Score</span>
                            <span class="value ${techBiasScore > 0 ? 'positive' : techBiasScore < 0 ? 'negative' : 'neutral'}">${techBiasScore > 0 ? '+' : ''}${techBiasScore}</span>
                        </div>`;
        if (techIndicators.length === 0) {
            techHtml += '<p style="color:#8892b0; font-size:0.75rem;">No indicators in this category.</p>';
        } else {
            techHtml += buildFullTable(techIndicators, data);
        }
        techCard.innerHTML = techHtml;
        twoColsDiv.appendChild(techCard);
        container.appendChild(twoColsDiv);

        // Now render the remaining cards (Economic Growth, Inflation, Jobs Market)
        rest.forEach(category => {
            const indicators = data.base_indicators.filter(ind => ind.category === category);
            const biasScore = data.category_bias[category] || 0;
            const biasObj = getBiasLabelAndClass(biasScore);
            const card = document.createElement('div');
            card.className = 'panel';
            let html = `<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                            <h3 style="margin:0;">${category}</h3>
                            <span class="value ${biasObj.class}" style="background:rgba(255,255,255,0.05); padding:2px 8px; border-radius:20px; font-size:0.7rem;">${biasObj.label}</span>
                        </div>
                        <div class="indicator-row" style="font-weight:bold; margin-bottom:2px">
                            <span>Bias Score</span>
                            <span class="value ${biasScore > 0 ? 'positive' : biasScore < 0 ? 'negative' : 'neutral'}">${biasScore > 0 ? '+' : ''}${biasScore}</span>
                        </div>`;
            if (indicators.length === 0) {
                html += '<p style="color:#8892b0; font-size:0.75rem;">No indicators in this category.</p>';
            } else {
                html += buildFullTable(indicators, data);
            }
            card.innerHTML = html;
            container.appendChild(card);
        });
    }

    function logout() { fetch('/logout').then(() => window.location.href = '/login'); }

    if (currencies.length > 0) {
        select.value = currencies[0];
        loadScorecard();
    }
    setTimeout(() => { if (typeof lucide !== 'undefined') lucide.createIcons(); }, 100);
</script>
</body>
</html>''')



    # Forex Scorecard template (correct)
    with open('templates/forex_scorecard.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Forex Scorecard - Tradion</title>
    <script src="https://unpkg.com/lucide@latest"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;display:flex}
        .sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100}
        .sidebar .logo{font-size:24px;font-weight:800;color:#00e5ff;text-align:center;margin-bottom:30px;padding-bottom:20px;border-bottom:1px solid #2a3040}
        .sidebar .menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:10px;cursor:pointer;transition:all 0.2s;color:#a0b0c0}
        .sidebar .menu-item:hover{background:rgba(0,229,255,0.08);color:#00e5ff}
        .sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;font-weight:bold}
        .sidebar .menu-icon{font-size:20px;margin-right:12px}
        .sidebar .menu-icon svg{width:20px;height:20px;vertical-align:middle}
        .main-content{flex:1;margin-left:280px;padding:12px 20px}
        .header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:10px}
        .header h2{color:#00e5ff;font-size:1.8rem}
        .symbol-selector select{padding:8px 12px;background:#121826;border:1.5px solid #2a3040;color:#fff;border-radius:8px;font-size:0.9rem;min-width:180px}
        .scorecard-grid{display:grid;grid-template-columns:0.9fr 1.1fr;gap:12px}
        .gauge-panel{
            background:#121826;
            border-radius:16px;
            padding:12px 12px 16px 12px;
            display:flex;
            flex-direction:column;
            align-items:center;
            border:1px solid #2a3040;
            position:relative;
        }
        .gauge-container{
            position:relative;
            width:160px;
            height:90px;
            margin-bottom:8px;
        }
        .gauge-svg{width:100%;height:100%}
        .gauge-bg{stroke:#2a3040;stroke-width:12;fill:none}
        .gauge-fill{stroke-width:12;fill:none;stroke-linecap:round;transition:stroke-dasharray 0.5s}
        .gauge-needle-group{transition:transform 0.5s ease-out}
        .needle-body{stroke:#fff;stroke-width:2;fill:none}
        .needle-tip{fill:#fff;stroke:#fff;stroke-width:1}
        .gauge-center{
            position:relative;
            bottom:auto;
            left:auto;
            transform:none;
            text-align:center;
            margin-top:4px;
        }
        .gauge-label{font-size:1.1rem;font-weight:bold;color:#00e5ff}
        .gauge-bias{font-size:0.85rem;color:#00e5a0}
        .loading-overlay{position:absolute;top:0;left:0;right:0;bottom:0;background:rgba(18,24,38,0.8);display:flex;align-items:center;justify-content:center;border-radius:16px;z-index:10}
        .loading-overlay.hidden{display:none}
        .spinner{border:3px solid #2a3040;border-top:3px solid #00e5ff;border-radius:50%;width:30px;height:30px;animation:spin 1s linear infinite}
        @keyframes spin{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}
        .score-summary{display:flex;flex-direction:column;gap:6px;width:100%;margin-top:8px}
        .score-item{display:flex;justify-content:space-between;align-items:center;padding:4px 10px;background:#1a1f2e;border-radius:8px}
        .score-label{font-weight:500;font-size:0.85rem}
        .score-value{font-weight:bold;padding:2px 8px;border-radius:20px;font-size:0.75rem}
        .score-value.positive{background:rgba(0,229,160,0.15);color:#00e5a0}
        .score-value.negative{background:rgba(255,77,109,0.15);color:#ff4d6d}
        .score-value.neutral{background:rgba(255,184,0,0.15);color:#ffb800}
        /* Right column cards */
        .right-col{display:flex;flex-direction:column;gap:10px}
        .two-cols{display:grid;grid-template-columns:1fr 1fr;gap:10px}
        .panel{
            background:#121826;
            border-radius:12px;
            padding:10px;
            border:1px solid #2a3040;
        }
        .panel h3{
            color:#00e5ff;
            font-size:0.9rem;
            margin-bottom:6px;
            border-bottom:1px solid #2a3040;
            padding-bottom:4px;
        }
        .indicator-row{
            display:flex;
            justify-content:space-between;
            padding:3px 0;
            border-bottom:1px solid rgba(42,48,64,0.3);
        }
        .indicator-row:last-child{border-bottom:none}
        .indicator-label{font-size:0.75rem}
        .indicator-values{display:flex;gap:8px;align-items:center}
        .value{font-weight:500;width:45px;text-align:right;font-size:0.75rem}
        .value.positive{color:#00e5a0}
        .value.negative{color:#ff4d6d}
        .value.neutral{color:#a0b0c0}
        .surprise{font-size:0.7rem;padding:1px 4px;border-radius:4px}
        .surprise.positive{background:rgba(0,229,160,0.15);color:#00e5a0}
        .surprise.negative{background:rgba(255,77,109,0.15);color:#ff4d6d}
        /* Table styles */
        .panel table {
            width: 100%;
            border-collapse: collapse;
        }
        .panel th, .panel td {
            padding: 4px 3px;
            text-align: left;
            font-size: 0.7rem;
        }
        .panel th:nth-child(1), .panel td:nth-child(1) { width: 40%; }
        .panel th:nth-child(2), .panel td:nth-child(2) { width: 15%; }
        .panel th:nth-child(3), .panel td:nth-child(3) { width: 15%; }
        .panel th:nth-child(4), .panel td:nth-child(4) { width: 15%; }
        .panel th:nth-child(5), .panel td:nth-child(5) { width: 15%; }
        .panel.crowd-table th:nth-child(1), .panel.crowd-table td:nth-child(1) { width: 50%; }
        .panel.crowd-table th:nth-child(2), .panel.crowd-table td:nth-child(2) { width: 50%; }
        /* Score History Chart */
        .history-chart-container {
            background: #121826;
            border-radius: 12px;
            padding: 10px;
            margin-top: 10px;
            border: 1px solid #2a3040;
        }
        .history-chart-container h3 {
            color: #00e5ff;
            font-size: 0.9rem;
            margin-bottom: 8px;
            border-bottom: 1px solid #2a3040;
            padding-bottom: 4px;
        }
        .chart-wrapper {
            position: relative;
            height: 240px;
            width: 100%;
        }
        @media (max-width:768px){
            .main-content{margin-left:0;padding:60px 15px 20px;}
            .scorecard-grid{grid-template-columns:1fr;gap:15px}
            .gauge-container{width:140px;height:80px}
            .panel{padding:8px}
            .indicator-values{gap:6px}
            .value{width:40px}
            .chart-wrapper{height: 200px;}
            .two-cols{grid-template-columns:1fr;}
        }
    </style>
</head>
<body>
<div class="sidebar">
    <div class="logo">⚡ Tradion</div>
    <div class="menu-item" onclick="window.location.href='/dashboard'"><i data-lucide="chart-line" class="menu-icon"></i><span>Dashboard</span></div>
    <div class="menu-item" onclick="window.location.href='/currencies'"><i data-lucide="currency" class="menu-icon"></i><span>COT Data</span></div>
    <div class="menu-item" onclick="window.location.href='/scorecard'"><i data-lucide="trending-up" class="menu-icon"></i><span>Asset Scorecard</span></div>
    <div class="menu-item active" onclick="window.location.href='/forex-scorecard'"><i data-lucide="trending-up" class="menu-icon"></i><span>Forex Scorecard</span></div>
    <div class="menu-item" onclick="window.location.href='/profile'"><i data-lucide="user" class="menu-icon"></i><span>Profile</span></div>
    <div class="menu-item" onclick="logout()"><i data-lucide="log-out" class="menu-icon"></i><span>Logout</span></div>
</div>

<div class="main-content">
    <div class="header">
        <h2>Forex Scorecard</h2>
        <div class="symbol-selector">
            <select id="symbolSelect" onchange="loadScorecard()">
                <option value="">Select Forex Pair...</option>
            </select>
        </div>
    </div>

    <div class="scorecard-grid">
        <div>
            <div class="gauge-panel" id="gaugePanel">
                <div class="loading-overlay" id="loadingOverlay"><div class="spinner"></div></div>
                <div class="gauge-container">
                    <svg class="gauge-svg" viewBox="0 0 220 110">
                        <defs><linearGradient id="gaugeGradient" x1="0%" y1="0%" x2="100%" y2="0%"><stop offset="0%" stop-color="#ff4d6d"/><stop offset="50%" stop-color="#ffb800"/><stop offset="100%" stop-color="#00e5a0"/></linearGradient><filter id="shadow" x="-20%" y="-20%" width="140%" height="140%"><feDropShadow dx="1" dy="1" stdDeviation="1" flood-color="#000" flood-opacity="0.5"/></filter></defs>
                        <path class="gauge-bg" d="M20,110 A90,90 0 0,1 200,110"/>
                        <path id="gaugeFill" class="gauge-fill" d="M20,110 A90,90 0 0,1 200,110" stroke="url(#gaugeGradient)"/>
                        <g id="gaugeNeedleGroup" class="gauge-needle-group" transform="rotate(-90,110,110)" filter="url(#shadow)"><line class="needle-body" x1="110" y1="110" x2="110" y2="30"/><circle class="needle-tip" cx="110" cy="30" r="4"/></g>
                    </svg>
                </div>
                <div class="gauge-center">
                    <div class="gauge-label" id="gaugeBias">Neutral</div>
                    <div class="gauge-bias" id="gaugeValue">0.0</div>
                </div>
                <div class="score-summary">
                    <div class="score-item"><span class="score-label">Tradion Score</span><span id="tradionScore" class="score-value neutral">0</span></div>
                    <div class="score-item"><span class="score-label">Technical</span><span id="technicalScore" class="score-value neutral">● 0</span></div>
                    <div class="score-item"><span class="score-label">COT Alignment</span><span id="sentimentCOTScore" class="score-value neutral">0</span></div>
                    <div class="score-item"><span class="score-label">Fundamentals</span><span id="fundamentalsScore" class="score-value neutral">0</span></div>
                </div>
            </div>

            <!-- Score History Chart -->
            <div class="history-chart-container">
                <h3>Tradion Score Overtime</h3>
                <div class="chart-wrapper">
                    <canvas id="scoreHistoryChart"></canvas>
                </div>
            </div>
        </div>
        <div class="right-col" id="categoryCards"></div>
    </div>
</div>

<script>
    const allPairs = {{ all_pairs|tojson }};
    const select = document.getElementById('symbolSelect');
    allPairs.forEach(pair => {
        const pairStr = pair[0] + '/' + pair[1];
        const opt = document.createElement('option');
        opt.value = pairStr;
        opt.textContent = pairStr;
        select.appendChild(opt);
    });
    const standaloneAssets = ['XAU', 'BTC'];

    // Order of categories on the right side (matching the screenshot)
    const CATEGORY_ORDER = [
        "Crowd Sentiment (COT)",
        "Technical Bias",
        "Economic Growth Bias",
        "Inflation Bias",
        "Jobs Market Bias"
    ];

    let historyChart = null;

    async function loadScorecard() {
        const symbol = select.value;
        if (!symbol) return;
        const overlay = document.getElementById('loadingOverlay');
        overlay.classList.remove('hidden');
        try {
            const res = await fetch('/api/asset_scorecard/' + encodeURIComponent(symbol));
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            const baseCurr = data.base;
            const quoteCurr = data.quote;
            const isStandalone = standaloneAssets.includes(baseCurr) && quoteCurr === 'USD';
            if (isStandalone) {
                updateGauge(data.overall.score, data.overall.bias, data.overall.color);
                updateScores(data);
                renderCategoryCards(data);
            } else {
                // For forex pairs, use the same data (technical_score from API is already correct)
                updateGauge(data.overall.score, data.overall.bias, data.overall.color);
                updateScores(data);
                renderCategoryCards(data);
            }
            loadScoreHistory(symbol);
        } catch(e) { console.error(e); document.getElementById('categoryCards').innerHTML = `<div class="error-message">Error: ${e.message}</div>`; }
        finally { overlay.classList.add('hidden'); }
    }

    async function loadScoreHistory(asset) {
        try {
            const res = await fetch('/api/score_history/' + encodeURIComponent(asset));
            const data = await res.json();
            renderHistoryChart(data.history);
        } catch(e) {
            console.error('Error loading score history:', e);
        }
    }

    function renderHistoryChart(history) {
        const canvas = document.getElementById('scoreHistoryChart');
        const ctx = canvas.getContext('2d');
        if (historyChart) historyChart.destroy();
        if (!history || history.length === 0) {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.font = "12px 'Segoe UI'";
            ctx.fillStyle = "#8892b0";
            ctx.textAlign = "center";
            ctx.fillText("No score history yet. Data will appear after the first scheduled save (every 3 days).", canvas.width/2, canvas.height/2);
            return;
        }
        const labels = history.map(h => h.date);
        const scores = history.map(h => h.score);
        historyChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Score',
                    data: scores,
                    backgroundColor: scores.map(s => s > 0 ? 'rgba(0, 229, 160, 0.7)' : (s < 0 ? 'rgba(255, 77, 109, 0.7)' : 'rgba(160, 160, 160, 0.5)')),
                    borderColor: scores.map(s => s > 0 ? '#00e5a0' : (s < 0 ? '#ff4d6d' : '#a0a0a0')),
                    borderWidth: 1,
                    borderRadius: 4,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: (ctx) => `Score: ${ctx.raw.toFixed(1)}` } }
                },
                scales: {
                    y: {
                        title: { display: true, text: 'Score', color: '#a0b0c0' },
                        ticks: { color: '#e0e0e0' },
                        grid: {
                            color: (context) => context.tick.value === 0 ? '#2a3040' : 'transparent',
                            lineWidth: (context) => context.tick.value === 0 ? 1 : 0
                        },
                        min: -20,
                        max: 20
                    },
                    x: {
                        title: { display: true, text: 'Date', color: '#a0b0c0' },
                        ticks: { color: '#e0e0e0', maxRotation: 45, autoSkip: true },
                        grid: { display: false }
                    }
                }
            }
        });
    }

    function updateGauge(score, bias, color) {
        const clampedScore = Math.min(Math.max(score, -10), 10);
        const angle = ((clampedScore + 10) / 20) * 180;
        document.getElementById('gaugeNeedleGroup').setAttribute('transform', `rotate(${angle - 90}, 110, 110)`);
        document.getElementById('gaugeBias').textContent = bias;
        document.getElementById('gaugeValue').textContent = score.toFixed(1);
        document.querySelector('.gauge-bias').style.color = color;
    }

    function updateScores(data) {
        setScoreValue('tradionScore', data.tradion_score);
        setScoreValue('technicalScore', data.technical_score > 0 ? '▲ 1' : (data.technical_score < 0 ? '▼ -1' : '● 0'));
        setScoreValue('sentimentCOTScore', data.sentiment_cot_score);
        setScoreValue('fundamentalsScore', data.fundamentals_score, data.fundamentals_score > 0 ? 'positive' : (data.fundamentals_score < 0 ? 'negative' : 'neutral'));
    }

    function setScoreValue(id, text, classNameOverride) {
        const el = document.getElementById(id);
        el.textContent = text;
        if (classNameOverride) el.className = 'score-value ' + classNameOverride;
        else {
            const val = parseFloat(text);
            el.className = 'score-value ' + (val > 0 ? 'positive' : val < 0 ? 'negative' : 'neutral');
        }
    }

    // Helper: map numeric bias score to label and CSS class
    function getBiasLabelAndClass(score) {
        if (score >= 5) return { label: 'Bullish', className: 'positive' };
        if (score <= -5) return { label: 'Bearish', className: 'negative' };
        return { label: 'Neutral', className: 'neutral' };
    }

    // Build full table with 5 columns
    function buildFullTable(indicators, data) {
        let html = `<div style="overflow-x:auto;">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th>Indicator</th>
                                    <th>Signal</th>
                                    <th>Actual</th>
                                    <th>Forecast</th>
                                    <th>Surprise</th>
                                </tr>
                            </thead>
                            <tbody>`;
        indicators.forEach(ind => {
            if (ind.name === '2-Year Bond Yield Trend') {
                const score = (ind.score !== undefined && ind.score !== null) ? Number(ind.score) : 0;
                let signal = score > 0 ? 'Bullish' : (score < 0 ? 'Bearish' : 'Neutral');
                let signalClass = score > 0 ? 'positive' : (score < 0 ? 'negative' : 'neutral');
                html += `<tr>
                            <td>${ind.name}</td>
                            <td class="value ${signalClass}">${signal}</td>
                            <td class="value">-</td><td class="value">-</td><td class="value">-</td>
                        </tr>`;
            }
            else if (ind.name === 'COT Alignment') {
                let score = (ind.score !== undefined && ind.score !== null) ? Number(ind.score) : 0;
                if (isNaN(score)) score = 0;
                let label = 'Neutral', labelClass = 'neutral';
                if (score > 0) { label = 'Bullish'; labelClass = 'positive'; }
                else if (score < 0) { label = 'Bearish'; labelClass = 'negative'; }
                html += `<tr>
                            <td>${ind.name}</td>
                            <td class="value ${labelClass}">${label}</td>
                            <td class="value">-</td><td class="value">-</td><td class="value">-</td>
                        </tr>`;
            }
            else {
                const surprise = ind.actual - ind.forecast;
                const better = ind.is_lower_better ? surprise < 0 : surprise > 0;
                const colorClass = better ? 'positive' : (surprise === 0 ? 'neutral' : 'negative');
                let signal = better ? 'Bullish' : (surprise === 0 ? 'Neutral' : 'Bearish');
                let signalClass = better ? 'positive' : (surprise === 0 ? 'neutral' : 'negative');
                html += `<tr>
                            <td>${ind.name}</td>
                            <td class="value ${signalClass}">${signal}</td>
                            <td class="value">${ind.actual}</td>
                            <td class="value">${ind.forecast}</td>
                            <td class="surprise ${colorClass}">${surprise > 0 ? '+' : ''}${surprise.toFixed(1)}</td>
                        </tr>`;
            }
        });
        html += `</tbody></table></div>`;
        return html;
    }

    // Simplified table for Crowd Sentiment (COT) – only Indicator and Signal
    function buildCrowdTable(data) {
        const cotInd = data.base_indicators.find(i => i.name === 'COT Alignment');
        if (!cotInd) return '<p>No data</p>';
        let score = (cotInd.score !== undefined && cotInd.score !== null) ? Number(cotInd.score) : 0;
        if (isNaN(score)) score = 0;
        let label = 'Neutral', labelClass = 'neutral';
        if (score > 0) { label = 'Bullish'; labelClass = 'positive'; }
        else if (score < 0) { label = 'Bearish'; labelClass = 'negative'; }
        return `<div style="overflow-x:auto;">
                    <table class="crowd-table">
                        <thead><tr><th>Indicator</th><th>Signal</th></tr></thead>
                        <tbody>
                            <tr><td>COT Alignment</td><td class="value ${labelClass}">${label}</td>
                        </tbody>
                    </table>
                </div>`;
    }

    function renderCategoryCards(data) {
        const container = document.getElementById('categoryCards');
        container.innerHTML = '';

        // First two cards: Crowd Sentiment (COT) and Technical Bias side by side
        const firstTwo = CATEGORY_ORDER.slice(0,2);
        const rest = CATEGORY_ORDER.slice(2);

        const twoColsDiv = document.createElement('div');
        twoColsDiv.className = 'two-cols';

        // Crowd Sentiment (COT) card
        const crowdCategory = firstTwo[0];
        const crowdBiasScore = data.category_bias[crowdCategory] || 0;
        const crowdBiasObj = getBiasLabelAndClass(crowdBiasScore);
        const crowdCard = document.createElement('div');
        crowdCard.className = 'panel';
        let crowdHtml = `<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                            <h3 style="margin:0;">${crowdCategory}</h3>
                            <span class="value ${crowdBiasObj.class}" style="background:rgba(255,255,255,0.05); padding:2px 8px; border-radius:20px; font-size:0.7rem;">${crowdBiasObj.label}</span>
                        </div>
                        <div class="indicator-row" style="font-weight:bold; margin-bottom:2px">
                            <span>Bias Score</span>
                            <span class="value ${crowdBiasScore > 0 ? 'positive' : crowdBiasScore < 0 ? 'negative' : 'neutral'}">${crowdBiasScore > 0 ? '+' : ''}${crowdBiasScore}</span>
                        </div>`;
        crowdHtml += buildCrowdTable(data);
        crowdCard.innerHTML = crowdHtml;
        twoColsDiv.appendChild(crowdCard);

        // Technical Bias card - use the 21-day SMA from data.technical_score
        const techCategory = firstTwo[1];
        const techBiasScore = data.category_bias[techCategory] || 0;
        const techBiasObj = getBiasLabelAndClass(techBiasScore);
        const techCard = document.createElement('div');
        techCard.className = 'panel';
        // Compute signal based on data.technical_score (which is +/-1 or 0)
        const techScore = data.technical_score || 0;
        const techSignal = techScore > 0 ? 'Bullish' : (techScore < 0 ? 'Bearish' : 'Neutral');
        const techSignalClass = techScore > 0 ? 'positive' : (techScore < 0 ? 'negative' : 'neutral');
        let techHtml = `<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                            <h3 style="margin:0;">${techCategory}</h3>
                            <span class="value ${techBiasObj.class}" style="background:rgba(255,255,255,0.05); padding:2px 8px; border-radius:20px; font-size:0.7rem;">${techBiasObj.label}</span>
                        </div>
                        <div class="indicator-row" style="font-weight:bold; margin-bottom:2px">
                            <span>Bias Score</span>
                            <span class="value ${techBiasScore > 0 ? 'positive' : techBiasScore < 0 ? 'negative' : 'neutral'}">${techBiasScore > 0 ? '+' : ''}${techBiasScore}</span>
                        </div>
                        <div class="indicator-row">
                            <span class="indicator-label">21-day SMA Trend</span>
                            <div class="indicator-values">
                                <span class="value ${techSignalClass}">${techSignal}</span>
                                <span class="value">-</span>
                                <span class="value">-</span>
                                <span class="value">-</span>
                            </div>
                        </div>`;
        techCard.innerHTML = techHtml;
        twoColsDiv.appendChild(techCard);
        container.appendChild(twoColsDiv);

        // Remaining cards: Economic Growth, Inflation, Jobs Market
        rest.forEach(category => {
            const indicators = data.base_indicators.filter(ind => ind.category === category);
            const biasScore = data.category_bias[category] || 0;
            const biasObj = getBiasLabelAndClass(biasScore);
            const card = document.createElement('div');
            card.className = 'panel';
            let html = `<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                            <h3 style="margin:0;">${category}</h3>
                            <span class="value ${biasObj.class}" style="background:rgba(255,255,255,0.05); padding:2px 8px; border-radius:20px; font-size:0.7rem;">${biasObj.label}</span>
                        </div>
                        <div class="indicator-row" style="font-weight:bold; margin-bottom:2px">
                            <span>Bias Score</span>
                            <span class="value ${biasScore > 0 ? 'positive' : biasScore < 0 ? 'negative' : 'neutral'}">${biasScore > 0 ? '+' : ''}${biasScore}</span>
                        </div>`;
            if (indicators.length === 0) {
                html += '<p style="color:#8892b0; font-size:0.75rem;">No indicators in this category.</p>';
            } else {
                html += buildFullTable(indicators, data);
            }
            card.innerHTML = html;
            container.appendChild(card);
        });
    }

    function logout() { fetch('/logout').then(()=>window.location.href='/login'); }
    if (allPairs.length > 0) { select.value = allPairs[0][0] + '/' + allPairs[0][1]; loadScorecard(); }
    setTimeout(() => { if (typeof lucide !== 'undefined') lucide.createIcons(); }, 100);
</script>
</body>
</html>''')


            # Admin (with Lucide icons, preserving all functionality)
    with open('templates/admin.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html><head><title>Admin - Tradion</title>
<script src="https://unpkg.com/lucide@latest"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI',sans-serif;background:#0B0F1A;color:#fff}.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;z-index:100}.logo{font-size:22px;color:#00e5ff;margin-bottom:30px}.menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:8px;cursor:pointer;color:#a0b0c0}.menu-item:hover{background:rgba(0,229,255,0.1);color:#00e5ff}.menu-icon{font-size:20px;margin-right:12px}.menu-icon svg{width:20px;height:20px;vertical-align:middle}.main-content{margin-left:280px;padding:20px}.card{background:#121826;border-radius:12px;padding:20px;margin-bottom:20px;border:1px solid #2a3040}button{padding:8px 16px;background:#00e5ff;color:#0B0F1A;border:none;border-radius:6px;cursor:pointer;font-weight:bold}button.secondary{background:transparent;border:1px solid #00e5ff;color:#00e5ff}.content-pane{display:none}.content-pane.active{display:block}input,select{padding:6px;background:#1a1f2e;border:1px solid #2a3040;color:#fff;border-radius:4px;width:100%;margin-bottom:10px}.modal{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.7);z-index:1000;justify-content:center;align-items:center}.modal-content{background:#121826;padding:25px;border-radius:16px;width:90%;max-width:500px;border:1px solid #2a3040}.month-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin:15px 0}

/* ===== Clean Tables (shared) ===== */
.table-container {
    overflow-x: auto;
    margin-top: 20px;
    border-radius: 12px;
    border: 1px solid #2a3040;
}
.clean-table {
    width: 100%;
    border-collapse: collapse;
    background: #121826;
    font-size: 14px;
}
.clean-table th,
.clean-table td {
    padding: 12px 10px;
    text-align: left;
    border-bottom: 1px solid #2a3040;
    vertical-align: middle;
}
.clean-table th {
    background: rgba(0, 229, 255, 0.15);
    color: #00e5ff;
    font-weight: 600;
    font-size: 0.9rem;
    position: sticky;
    top: 0;
}
.clean-table td {
    color: #e0e0e0;
}
.clean-table tr:hover {
    background: rgba(0, 229, 255, 0.05);
}

/* COT specific */
.form-row {
    display: flex;
    gap: 15px;
    align-items: flex-end;
    flex-wrap: wrap;
}
.form-group {
    flex: 1;
    min-width: 120px;
}
.positive { color: #00e5a0; }
.negative { color: #ff4d6d; }

/* ===== Mobile / Responsive Styles ===== */
.hamburger {
    display: none;
    font-size: 28px;
    cursor: pointer;
    color: #00e5ff;
    position: fixed;
    top: 15px;
    left: 20px;
    z-index: 1100;
    background: #121826;
    padding: 8px 12px;
    border-radius: 8px;
    border: 1px solid #2a3040;
}

@media (max-width: 768px) {
    .sidebar {
        transform: translateX(-100%);
        transition: transform 0.3s ease;
        width: 260px;
        z-index: 1050;
        position: fixed;
        top: 0;
        left: 0;
        height: 100%;
        background: #121826;
    }
    .sidebar.open {
        transform: translateX(0);
    }
    .main-content {
        margin-left: 0 !important;
        padding: 60px 15px 20px 15px !important;
        width: 100%;
    }
    .hamburger {
        display: block;
    }
    .navbar {
        flex-direction: column;
        align-items: flex-start;
        gap: 10px;
        padding: 10px 15px;
    }
    .clean-table th, .clean-table td {
        padding: 8px 6px;
        font-size: 12px;
    }
    .form-row {
        flex-direction: column;
        gap: 10px;
    }
}
</style>
</head>
<body>
<div class="hamburger" onclick="toggleSidebar()">☰</div>
<div class="sidebar">
    <div class="logo">👑 ADMIN PANEL</div>
    <div class="menu-item" onclick="showPane('cotUpload')"><i data-lucide="folder-open" class="menu-icon"></i> COT Upload</div>
    <div class="menu-item" onclick="showPane('econUpload')"><i data-lucide="file-spreadsheet" class="menu-icon"></i> Econ Upload</div>
    <div class="menu-item" onclick="showPane('econIndicators')"><i data-lucide="bar-chart-2" class="menu-icon"></i> Indicators</div>
    <div class="menu-item" onclick="showPane('cotData')"><i data-lucide="database" class="menu-icon"></i> COT Data</div>
    <div class="menu-item" onclick="showPane('seasonality')"><i data-lucide="calendar" class="menu-icon"></i> Seasonality</div>
    <div class="menu-item" onclick="showPane('sentiment')"><i data-lucide="message-circle" class="menu-icon"></i> Sentiment</div>
    <div class="menu-item" onclick="showPane('users')"><i data-lucide="users" class="menu-icon"></i> Users</div>
    <div class="menu-item" onclick="window.location.href='/dashboard'"><i data-lucide="arrow-left" class="menu-icon"></i> Dashboard</div>
</div>
<div class="main-content">
    <div id="cotUploadPane" class="content-pane active"><div class="card"><h2>Upload COT Data</h2><input type="file" id="cotFile" accept=".xlsx"><button onclick="uploadCOT()"><i data-lucide="upload" style="width:16px;height:16px;margin-right:6px"></i> Upload</button><div id="cotStatus" style="margin-top:10px"></div></div></div>
    <div id="econUploadPane" class="content-pane"><div class="card"><h2>Upload Economic Data</h2><input type="file" id="econFile" accept=".xlsx"><button onclick="uploadEcon()"><i data-lucide="upload" style="width:16px;height:16px;margin-right:6px"></i> Upload</button><div id="econStatus" style="margin-top:10px"></div></div></div>
    
    <!-- Economic Indicators (with clean table) -->
    <div id="econIndicatorsPane" class="content-pane">
        <div class="card">
            <h2>Economic Indicators</h2>
            <label>Currency: <select id="econCurrencySelect" onchange="loadIndicators()"><option value="">-- choose --</option></select></label>
            <div style="margin:15px 0"><button onclick="openIndicatorModal()"><i data-lucide="plus" style="width:16px;height:16px;margin-right:6px"></i> Add Indicator</button></div>
            <div id="indicatorsList"></div>
        </div>
    </div>
    
    <!-- ========== HISTORICAL COT DATA PANE ========== -->
    <div id="cotDataPane" class="content-pane">
        <div class="card">
            <h2>Manage Historical COT Data</h2>
            <div style="display:flex; gap:15px; flex-wrap:wrap; align-items:flex-end; margin-bottom:20px">
                <div style="flex:1; min-width:150px">
                    <label>Currency</label>
                    <select id="cotCurrency" onchange="loadHistoryForCurrency()">
                        <option value="">-- Select --</option>
                        <option value="EUR">EUR</option>
                        <option value="USD">USD</option>
                        <option value="AUD">AUD</option>
                        <option value="CHF">CHF</option>
                        <option value="GBP">GBP</option>
                        <option value="JPY">JPY</option>
                        <option value="CAD">CAD</option>
                        <option value="NZD">NZD</option>
                        <option value="XAU">XAU</option>
                        <option value="BTC">BTC</option>
                    </select>
                </div>
                <div>
                    <button onclick="loadHistoryForCurrency()" class="secondary"><i data-lucide="refresh-cw" style="width:16px;height:16px;margin-right:6px"></i> Refresh</button>
                </div>
            </div>

            <!-- Form to add new record -->
            <div style="background:#1a1f2e; padding:15px; border-radius:8px; margin-bottom:20px">
                <h3>Add New Weekly COT Record</h3>
                <div class="form-row">
                    <div class="form-group">
                        <label>Date (Friday)</label>
                        <input type="date" id="newReportDate" required>
                    </div>
                    <div class="form-group">
                        <label>Longs (Contracts)</label>
                        <input type="number" step="1" id="newLongs" placeholder="e.g. 150000">
                    </div>
                    <div class="form-group">
                        <label>Shorts (Contracts)</label>
                        <input type="number" step="1" id="newShorts" placeholder="e.g. 120000">
                    </div>
                    <div class="form-group">
                        <label>Δ Longs</label>
                        <input type="number" step="1" id="newChangeLongs" placeholder="Change in Longs">
                    </div>
                    <div class="form-group">
                        <label>Δ Shorts</label>
                        <input type="number" step="1" id="newChangeShorts" placeholder="Change in Shorts">
                    </div>
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label>Net Position (auto)</label>
                        <input type="text" id="newNet" readonly disabled>
                    </div>
                    <div class="form-group">
                        <label>Weekly Change (auto)</label>
                        <input type="text" id="newWeeklyChange" readonly disabled>
                    </div>
                    <div>
                        <button onclick="addHistoricalRecord()"><i data-lucide="plus" style="width:16px;height:16px;margin-right:6px"></i> Add Record</button>
                    </div>
                </div>
            </div>

            <!-- Table of existing records -->
            <h3>Historical Records</h3>
            <div class="table-container">
                <table class="clean-table">
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Longs</th>
                            <th>Shorts</th>
                            <th>Net Position</th>
                            <th>Δ Longs</th>
                            <th>Δ Shorts</th>
                            <th>Weekly Change</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="cotHistoryTableBody">
                        <tr><td colspan="8" style="text-align:center">Select a currency to view records</tbody>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    <!-- ========== END COT DATA PANE ========== -->
    
    <div id="seasonalityPane" class="content-pane"><div class="card"><h2>Seasonality Configuration</h2><label>Pair: <select id="seasonPairSelect" onchange="loadSeasonConfig()"><option value="">-- select pair --</option></select></label><h3 style="margin:20px 0 10px">Monthly Bias</h3><div id="monthlyBiasContainer"></div><button onclick="saveMonthlyBiases()"><i data-lucide="save" style="width:16px;height:16px;margin-right:6px"></i> Save Monthly Biases</button><h3 style="margin:20px 0 10px">Date Ranges</h3><button onclick="addDateRange()"><i data-lucide="plus" style="width:16px;height:16px;margin-right:6px"></i> Add Date Range</button><div id="dateRangeList" style="margin-top:10px"></div></div></div>
    
    <!-- Sentiment Data (with clean table) -->
    <div id="sentimentPane" class="content-pane">
        <div class="card">
            <h2>Sentiment Data</h2>
            <div style="margin-bottom:15px"><button onclick="openSentimentModal()"><i data-lucide="plus" style="width:16px;height:16px;margin-right:6px"></i> Add/Update Sentiment</button></div>
            <div id="sentimentList"></div>
        </div>
    </div>
    
    <div id="usersPane" class="content-pane"><div class="card"><h2>User Management</h2><button onclick="loadUsers()"><i data-lucide="refresh-cw" style="width:16px;height:16px;margin-right:6px"></i> Refresh</button><div id="usersList" style="margin-top:15px"></div></div></div>
</div>

<!-- Indicator Modal -->
<div class="modal" id="indicatorModal"><div class="modal-content"><h3 id="indModalTitle" style="color:#00e5ff;margin-bottom:20px">Add Indicator</h3><form id="indicatorForm"><input type="hidden" id="indicatorId"><label>Currency</label><select id="indCurrencySelect" required></select><label>Indicator Name</label><input type="text" id="indName" required><div style="display:grid;grid-template-columns:1fr 1fr;gap:10px"><div><label>Forecast</label><input type="number" step="0.1" id="indForecast" required></div><div><label>Actual</label><input type="number" step="0.1" id="indActual" required></div></div><label><input type="checkbox" id="indLowerBetter"> Lower is better</label><label>Category</label><select id="indCategory" required><option value="Technical Bias">Technical Bias</option><option value="Economic Growth Bias">Economic Growth Bias</option><option value="Inflation Bias">Inflation Bias</option><option value="Jobs Market Bias">Jobs Market Bias</option><option value="Crowd Sentiment (COT)">Crowd Sentiment (COT)</option></select><div style="display:flex;gap:10px;margin-top:20px"><button type="submit"><i data-lucide="save" style="width:16px;height:16px;margin-right:6px"></i> Save</button><button type="button" onclick="closeIndicatorModal()" class="secondary">Cancel</button></div></form></div></div>

<!-- Sentiment Modal -->
<div class="modal" id="sentimentModal">
  <div class="modal-content">
    <h3 style="color:#00e5ff;margin-bottom:20px">Sentiment Entry</h3>
    <form id="sentimentForm">
      <label>Pair</label>
      <select id="sentPair" required></select>
      <label>Long % (0-100)</label>
      <input type="number" step="0.1" id="sentLong" min="0" max="100" required>
      <small>Short % will be automatically calculated as 100 - Long %</small>
      <div style="display:flex;gap:10px;margin-top:20px">
        <button type="submit"><i data-lucide="save" style="width:16px;height:16px;margin-right:6px"></i> Save</button>
        <button type="button" onclick="closeSentimentModal()" class="secondary">Cancel</button>
      </div>
    </form>
  </div>
</div>

<script>
const allPairs={{ ALL_PAIRS|tojson }};
const currencies=["USD","EUR","GBP","JPY","AUD","CAD","CHF","NZD","XAU","BTC"];
let seasonCurrentPair='';
let currentHistory = [];

function toggleSidebar() {
    document.getElementById('sidebar').classList.toggle('open');
}

function showPane(pane){
    document.querySelectorAll('.content-pane').forEach(p=>p.classList.remove('active'));
    document.getElementById(pane+'Pane').classList.add('active');
    if(pane==='users') loadUsers();
    if(pane==='seasonality') populateSeasonPairs();
    if(pane==='econIndicators') populateCurrencySelects();
    if(pane==='cotData') loadHistoryForCurrency();
    if(pane==='sentiment') loadSentiment();
}

function populateCurrencySelects(){
    const s1=document.getElementById('econCurrencySelect');
    const s2=document.getElementById('indCurrencySelect');
    [s1,s2].forEach(sel=>{
        if(!sel) return;
        sel.innerHTML='<option value="">-- choose --</option>';
        currencies.forEach(c=>{const o=document.createElement('option');o.value=c;o.text=c;sel.appendChild(o)});
    });
}

async function loadIndicators(){
    const cur=document.getElementById('econCurrencySelect').value;
    if(!cur){
        document.getElementById('indicatorsList').innerHTML='';
        return;
    }
    const res=await fetch('/admin/econ_indicators?currency='+cur);
    const data=await res.json();
    if(!data.length){
        document.getElementById('indicatorsList').innerHTML='<div class="table-container"><div style="padding:20px;text-align:center">No indicators for this currency.</div></div>';
        return;
    }
    let html='<div class="table-container"><table class="clean-table"><thead><tr><th>Indicator</th><th>Forecast</th><th>Actual</th><th>Category</th><th>Lower</th><th>Actions</th><tr></thead><tbody>';
    data.forEach(ind=>{
        html+=`<tr>
            <td>${ind.indicator_name}</td>
            <td>${ind.forecast}</td>
            <td>${ind.actual}</td>
            <td>${ind.category||'General'}</td>
            <td>${ind.is_lower_better?'Yes':'No'}</td>
            <td><button onclick="editIndicator(${ind.id})"><i data-lucide="edit-2" style="width:14px;height:14px;margin-right:4px"></i>Edit</button> <button onclick="deleteIndicator(${ind.id})"><i data-lucide="trash-2" style="width:14px;height:14px;margin-right:4px"></i>Del</button></td>
        </tr>`;
    });
    html+='</tbody></table></div>';
    document.getElementById('indicatorsList').innerHTML=html;
    setTimeout(() => { if (typeof lucide !== 'undefined') lucide.createIcons(); }, 50);
}

function openIndicatorModal(id=null){
    document.getElementById('indicatorForm').reset();
    document.getElementById('indicatorId').value='';
    if(id){
        const ind=window._currentIndicators.find(i=>i.id===id);
        if(!ind) return;
        document.getElementById('indModalTitle').innerText='Edit Indicator';
        document.getElementById('indicatorId').value=ind.id;
        document.getElementById('indCurrencySelect').value=ind.currency||'';
        document.getElementById('indName').value=ind.indicator_name;
        document.getElementById('indForecast').value=ind.forecast;
        document.getElementById('indActual').value=ind.actual;
        document.getElementById('indLowerBetter').checked=ind.is_lower_better;
        document.getElementById('indCategory').value=ind.category||'General';
    } else document.getElementById('indModalTitle').innerText='Add Indicator';
    document.getElementById('indicatorModal').style.display='flex';
}
function closeIndicatorModal(){document.getElementById('indicatorModal').style.display='none';}
async function editIndicator(id){ const res=await fetch('/admin/econ_indicators?currency='+document.getElementById('econCurrencySelect').value); window._currentIndicators=await res.json(); openIndicatorModal(id); }
async function deleteIndicator(id){if(confirm('Delete?')){await fetch('/admin/econ_indicators/'+id,{method:'DELETE'});loadIndicators();}}
document.getElementById('indicatorForm').addEventListener('submit',async e=>{
    e.preventDefault();
    const id=document.getElementById('indicatorId').value;
    const data={currency:document.getElementById('indCurrencySelect').value,indicator_name:document.getElementById('indName').value,forecast:parseFloat(document.getElementById('indForecast').value),actual:parseFloat(document.getElementById('indActual').value),is_lower_better:document.getElementById('indLowerBetter').checked,category:document.getElementById('indCategory').value};
    const url=id?'/admin/econ_indicators/'+id:'/admin/econ_indicators';
    const method=id?'PUT':'POST';
    await fetch(url,{method,headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    closeIndicatorModal(); loadIndicators();
});

// ========== COT HISTORICAL MANAGEMENT ==========
async function loadHistoryForCurrency() {
    const currency = document.getElementById('cotCurrency').value;
    if (!currency) {
        document.getElementById('cotHistoryTableBody').innerHTML = '<tr><td colspan="8">Select a currency</tbody>';
        return;
    }
    const tbody = document.getElementById('cotHistoryTableBody');
    tbody.innerHTML = '<tr><td colspan="8">Loading...</tbody>';
    try {
        const res = await fetch(`/api/cot/history/${currency}`);
        if (!res.ok) throw new Error('Failed to load');
        const data = await res.json();
        currentHistory = data.history || [];
        if (!currentHistory.length) {
            tbody.innerHTML = '<tr><td colspan="8">No records yet. Add your first record below.</tbody>';
            return;
        }
        let html = '';
        currentHistory.sort((a,b) => new Date(b.date) - new Date(a.date));
        currentHistory.forEach(rec => {
            const date = rec.date.split('T')[0];
            const longs = rec.longs?.toLocaleString() || 0;
            const shorts = rec.shorts?.toLocaleString() || 0;
            const net = rec.net?.toLocaleString() || 0;
            const netClass = rec.net > 0 ? 'positive' : (rec.net < 0 ? 'negative' : '');
            const changeLongs = (rec.change_longs !== undefined && rec.change_longs !== null) ? 
                (rec.change_longs > 0 ? '+' : '') + rec.change_longs.toLocaleString() : '-';
            const changeShorts = (rec.change_shorts !== undefined && rec.change_shorts !== null) ? 
                (rec.change_shorts > 0 ? '+' : '') + rec.change_shorts.toLocaleString() : '-';
            const weeklyChange = (rec.change_net !== undefined && rec.change_net !== null) ? 
                (rec.change_net > 0 ? '+' : '') + rec.change_net.toLocaleString() : '-';
            const chlClass = (rec.change_longs > 0) ? 'positive' : (rec.change_longs < 0 ? 'negative' : '');
            const chsClass = (rec.change_shorts > 0) ? 'positive' : (rec.change_shorts < 0 ? 'negative' : '');
            const chnClass = (rec.change_net > 0) ? 'positive' : (rec.change_net < 0 ? 'negative' : '');
            html += `<tr>
                <td>${date}</td>
                <td>${longs}</td>
                <td>${shorts}</td>
                <td class="${netClass}">${net}</td>
                <td class="${chlClass}">${changeLongs}</td>
                <td class="${chsClass}">${changeShorts}</td>
                <td class="${chnClass}">${weeklyChange}</td>
                <td><button class="secondary" onclick="deleteRecord(${rec.id})"><i data-lucide="trash-2" style="width:14px;height:14px"></i> Delete</button></td>
            </tr>`;
        });
        tbody.innerHTML = html;
        setTimeout(() => { if (typeof lucide !== 'undefined') lucide.createIcons(); }, 50);
    } catch (err) {
        tbody.innerHTML = `<tr><td colspan="8">Error: ${err.message}</tbody>`;
    }
}

async function addHistoricalRecord() {
    const currency = document.getElementById('cotCurrency').value;
    if (!currency) {
        alert('Please select a currency first.');
        return;
    }
    const reportDate = document.getElementById('newReportDate').value;
    const longs = parseFloat(document.getElementById('newLongs').value);
    const shorts = parseFloat(document.getElementById('newShorts').value);
    const changeLongs = parseFloat(document.getElementById('newChangeLongs').value);
    const changeShorts = parseFloat(document.getElementById('newChangeShorts').value);
    if (!reportDate || isNaN(longs) || isNaN(shorts) || isNaN(changeLongs) || isNaN(changeShorts)) {
        alert('Please fill all fields (Date, Longs, Shorts, Δ Longs, Δ Shorts).');
        return;
    }
    const data = {
        currency: currency,
        report_date: reportDate,
        longs: longs,
        shorts: shorts,
        change_longs: changeLongs,
        change_shorts: changeShorts
    };
    try {
        const res = await fetch('/admin/cot_history', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data)
        });
        const result = await res.json();
        if (result.success) {
            alert(`Record for ${currency} added. Current COT data updated.`);
            document.getElementById('newReportDate').value = '';
            document.getElementById('newLongs').value = '';
            document.getElementById('newShorts').value = '';
            document.getElementById('newChangeLongs').value = '';
            document.getElementById('newChangeShorts').value = '';
            document.getElementById('newNet').value = '';
            document.getElementById('newWeeklyChange').value = '';
            loadHistoryForCurrency();
        } else {
            alert('Error: ' + (result.error || 'Unknown'));
        }
    } catch(err) {
        alert('Request failed: ' + err.message);
    }
}

async function deleteRecord(recordId) {
    if (!confirm('Delete this record?')) return;
    try {
        const res = await fetch(`/admin/cot_history/${recordId}`, {method: 'DELETE'});
        const result = await res.json();
        if (result.success) {
            alert('Record deleted');
            loadHistoryForCurrency();
        } else {
            alert('Error: ' + (result.error || 'Unknown'));
        }
    } catch(err) {
        alert('Request failed: ' + err.message);
    }
}

function updatePreview() {
    const longs = parseFloat(document.getElementById('newLongs').value) || 0;
    const shorts = parseFloat(document.getElementById('newShorts').value) || 0;
    const changeLongs = parseFloat(document.getElementById('newChangeLongs').value) || 0;
    const changeShorts = parseFloat(document.getElementById('newChangeShorts').value) || 0;
    const net = longs - shorts;
    const weeklyChange = changeLongs - changeShorts;
    document.getElementById('newNet').value = net;
    document.getElementById('newWeeklyChange').value = (weeklyChange > 0 ? '+' : '') + weeklyChange;
}
document.getElementById('newLongs').addEventListener('input', updatePreview);
document.getElementById('newShorts').addEventListener('input', updatePreview);
document.getElementById('newChangeLongs').addEventListener('input', updatePreview);
document.getElementById('newChangeShorts').addEventListener('input', updatePreview);
document.getElementById('cotCurrency').addEventListener('change', () => {
    loadHistoryForCurrency();
    updatePreview();
});

// ========== END COT HISTORICAL MANAGEMENT ==========

async function uploadCOT(){
    const file=document.getElementById('cotFile').files[0];
    if(!file) return;
    const fd=new FormData(); fd.append('file',file);
    const res=await fetch('/admin/cot/upload',{method:'POST',body:fd});
    const data=await res.json();
    document.getElementById('cotStatus').innerHTML=data.success?'✅ Uploaded':'❌ Error';
}
async function uploadEcon(){
    const file=document.getElementById('econFile').files[0];
    if(!file) return;
    const fd=new FormData(); fd.append('file',file);
    const res=await fetch('/admin/econ/upload',{method:'POST',body:fd});
    const data=await res.json();
    document.getElementById('econStatus').innerHTML=data.success?'✅ Uploaded':'❌ Error';
}
async function loadUsers(){
    const res=await fetch('/admin/users');
    const data=await res.json();
    let html='<div class="table-container"><table class="clean-table"><thead><tr><th>User</th><th>Email</th><th>Admin</th><th>Status</th><th>Actions</th></tr></thead><tbody>';
    data.users.forEach(u=>{
        const status=u.is_active?'Active':'Inactive';
        const badgeColor=u.is_active?'#00e5a0':'#ff4d6d';
        const actionBtn=u.is_active?`<button onclick="deactivateUser(${u.id})" class="secondary"><i data-lucide="user-x" style="width:14px;height:14px;margin-right:4px"></i>Deactivate</button>`:`<button onclick="activateUser(${u.id})"><i data-lucide="user-check" style="width:14px;height:14px;margin-right:4px"></i>Activate</button>`;
        html+=`<tr>
            <td>${u.username}</td>
            <td>${u.email}</td>
            <td>${u.is_admin?'Yes':'No'}</td>
            <td style="color:${badgeColor}">${status}</td>
            <td>${actionBtn}</td>
        </tr>`;
    });
    html+='</tbody></table></div>';
    document.getElementById('usersList').innerHTML=html;
    setTimeout(() => { if (typeof lucide !== 'undefined') lucide.createIcons(); }, 50);
}
async function activateUser(id){await fetch(`/admin/users/${id}/activate`,{method:'POST'});loadUsers();}
async function deactivateUser(id){await fetch(`/admin/users/${id}/deactivate`,{method:'POST'});loadUsers();}

// Seasonality (unchanged)
function populateSeasonPairs(){
    const select=document.getElementById('seasonPairSelect');
    select.innerHTML='<option value="">-- select pair --</option>';
    allPairs.forEach(p=>{const pairStr=p[0]+'/'+p[1];const opt=document.createElement('option');opt.value=pairStr;opt.textContent=pairStr;select.appendChild(opt)});
    seasonCurrentPair='';
    document.getElementById('monthlyBiasContainer').innerHTML='';
    document.getElementById('dateRangeList').innerHTML='';
}
async function loadSeasonConfig(){
    const pair=document.getElementById('seasonPairSelect').value;
    if(!pair) return;
    seasonCurrentPair=pair;
    const res=await fetch('/admin/seasonality/monthly/'+encodeURIComponent(pair));
    const biases=await res.json();
    let html='<div class="month-grid">';
    for(let m=1;m<=12;m++){
        const bias=biases[m]||'Neutral';
        html+=`<div><label>${new Date(2000,m-1).toLocaleString('default',{month:'short'})}</label><select id="month_${m}"><option value="Bullish" ${bias==='Bullish'?'selected':''}>Bullish</option><option value="Bearish" ${bias==='Bearish'?'selected':''}>Bearish</option><option value="Neutral" ${bias==='Neutral'?'selected':''}>Neutral</option></select></div>`;
    }
    html+='</div>';
    document.getElementById('monthlyBiasContainer').innerHTML=html;
    const res2=await fetch('/admin/seasonality/daterange/'+encodeURIComponent(pair));
    const ranges=await res2.json();
    renderDateRanges(ranges);
}
async function saveMonthlyBiases(){
    const pair=seasonCurrentPair;
    if(!pair) return;
    const data={};
    for(let m=1;m<=12;m++){data[m]=document.getElementById('month_'+m).value;}
    await fetch('/admin/seasonality/monthly/'+encodeURIComponent(pair),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    alert('Monthly biases saved.');
}
function renderDateRanges(ranges){
    let html='<div class="table-container"><table class="clean-table"><thead><tr><th>Start MM/DD</th><th>End MM/DD</th><th>Bias</th><th>Actions</th></tr></thead><tbody>';
    ranges.forEach(r=>{html+=`<tr>
        <td>${r.start_month}/${r.start_day}</td>
        <td>${r.end_month}/${r.end_day}</td>
        <td>${r.bias}</td>
        <td><button onclick="deleteDateRange(${r.id})"><i data-lucide="trash-2" style="width:14px;height:14px;margin-right:4px"></i>Delete</button></td>
    </tr>`});
    html+='</tbody></table></div>';
    document.getElementById('dateRangeList').innerHTML=html;
    setTimeout(() => { if (typeof lucide !== 'undefined') lucide.createIcons(); }, 50);
}
async function addDateRange(){
    const pair=seasonCurrentPair;
    if(!pair) return;
    const start_month=prompt('Start month (1-12):');
    const start_day=prompt('Start day:');
    const end_month=prompt('End month (1-12):');
    const end_day=prompt('End day:');
    const bias=prompt('Bias (Bullish/Bearish):','Bullish');
    if(!start_month||!start_day||!end_month||!end_day) return;
    const data={start_month,start_day,end_month,end_day,bias};
    const res=await fetch('/admin/seasonality/daterange/'+encodeURIComponent(pair),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    if(res.ok) loadSeasonConfig();
}
async function deleteDateRange(id){ await fetch('/admin/seasonality/daterange/'+id,{method:'DELETE'}); loadSeasonConfig(); }

// Sentiment (with clean table)
async function loadSentiment(){
    const res=await fetch('/admin/sentiment');
    const data=await res.json();
    if(!data.length){
        document.getElementById('sentimentList').innerHTML='<div class="table-container"><div style="padding:20px;text-align:center">No sentiment entries.</div></div>';
        return;
    }
    let html='<div class="table-container"><table class="clean-table"><thead><tr><th>Pair</th><th>Long %</th><th>Short %</th><th>Actions</th></td></thead><tbody>';
    data.forEach(s=>{
        html+=`<tr>
            <td>${s.pair}</td>
            <td>${s.long_pct}%</td>
            <td>${s.short_pct}%</td>
            <td><button onclick="editSentiment('${s.pair}',${s.long_pct},${s.short_pct})"><i data-lucide="edit-2" style="width:14px;height:14px;margin-right:4px"></i>Edit</button> <button onclick="deleteSentiment(${s.id})"><i data-lucide="trash-2" style="width:14px;height:14px;margin-right:4px"></i>Del</button></td>
        </tr>`;
    });
    html+='</tbody></table></div>';
    document.getElementById('sentimentList').innerHTML=html;
    setTimeout(() => { if (typeof lucide !== 'undefined') lucide.createIcons(); }, 50);
}
function openSentimentModal(){
    document.getElementById('sentimentForm').reset();
    document.getElementById('sentimentModal').style.display='flex';
    const select=document.getElementById('sentPair');
    select.innerHTML='';
    allPairs.forEach(p=>{const pairStr=p[0]+'/'+p[1];const opt=document.createElement('option');opt.value=pairStr;opt.textContent=pairStr;select.appendChild(opt)});
}
function closeSentimentModal(){document.getElementById('sentimentModal').style.display='none';}
function editSentiment(pair, long, short){
    openSentimentModal();
    document.getElementById('sentPair').value=pair;
    document.getElementById('sentLong').value=long;
}
async function deleteSentiment(id){ if(confirm('Delete?')){await fetch('/admin/sentiment/'+id,{method:'DELETE'});loadSentiment();} }
document.getElementById('sentimentForm').addEventListener('submit',async e=>{
    e.preventDefault();
    const long_pct = parseFloat(document.getElementById('sentLong').value);
    const short_pct = 100 - long_pct;
    const data={
        pair:document.getElementById('sentPair').value,
        long_pct: long_pct,
        short_pct: short_pct
    };
    await fetch('/admin/sentiment',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    closeSentimentModal(); loadSentiment();
});

// Initialisation
populateCurrencySelects();
loadUsers();
setTimeout(() => { if (typeof lucide !== 'undefined') lucide.createIcons(); }, 100);
</script>
</body>
</html>''')

            # Currencies (COT Data with bar chart + historical) – Lucide icons without layout break
    with open('templates/currencies.html', 'w') as f:
        f.write('''<!DOCTYPE html>
<html><head><title>COT Data - Tradion</title><meta name="viewport" content="width=device-width, initial-scale=1.0">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://unpkg.com/lucide@latest"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;display:flex}
.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100;transition:width 0.3s ease, transform 0.3s ease;overflow-x:hidden;white-space:nowrap}
.sidebar .logo{display:flex;justify-content:space-between;align-items:center;font-size:22px;font-weight:800;color:#00e5ff;margin-bottom:25px;padding-bottom:15px;border-bottom:1px solid #2a3040}
.sidebar .logo-text{transition:opacity 0.2s}
.sidebar.collapsed .logo-text{opacity:0;width:0;visibility:hidden}
.sidebar-toggle{background:none;border:none;color:#00e5ff;font-size:18px;cursor:pointer;padding:4px 8px;border-radius:6px;transition:0.2s}
.sidebar-toggle:hover{background:rgba(0,229,255,0.2)}
.sidebar .menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:10px;cursor:pointer;transition:all 0.2s;color:#a0b0c0}
.sidebar .menu-item:hover{background:rgba(0,229,255,0.08);color:#00e5ff}
.sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;font-weight:bold}
.sidebar .menu-icon{font-size:20px;margin-right:12px;transition:margin 0.2s}
.sidebar.collapsed .menu-icon{margin-right:0}
.sidebar .menu-item span:not(.menu-icon){transition:opacity 0.2s}
.sidebar.collapsed .menu-item span:not(.menu-icon){opacity:0;width:0;display:none}
/* Ensure Lucide SVG icons match the original icon size */
.sidebar .menu-icon svg {
    width: 20px;
    height: 20px;
    vertical-align: middle;
}
.main-content{flex:1;margin-left:280px;padding:20px 30px;transition:margin-left 0.3s}
.navbar{background:#121826;padding:15px 25px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #2a3040;border-radius:0 0 16px 16px;margin-bottom:20px}
.navbar-title{font-size:18px;color:#00e5ff}
button{padding:10px 20px;background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;border:none;border-radius:8px;cursor:pointer;font-weight:bold;transition:all 0.2s}
button:hover{transform:scale(1.02);box-shadow:0 0 15px rgba(0,229,255,0.3)}
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
.chart-container{background:#121826;border-radius:16px;padding:20px;margin-top:30px;border:1px solid #2a3040}
.chart-container h3{color:#00e5ff;margin-bottom:15px;font-size:1.2rem}
canvas{max-height:400px;width:100%}
.historical-section{margin-top:40px;padding-top:20px;border-top:1px solid #2a3040}
.dropdown-selector{padding:10px 15px;background:#1a1f2e;border:1px solid #2a3040;border-radius:8px;color:#fff;font-size:14px;margin-left:15px;cursor:pointer}
.dropdown-selector:focus{outline:none;border-color:#00e5ff}
.badge{display:inline-block;padding:4px 12px;border-radius:20px;font-size:12px;font-weight:bold;margin-left:10px}
.badge-bullish{background:rgba(0,229,160,0.2);color:#00e5a0}
.badge-bearish{background:rgba(255,77,109,0.2);color:#ff4d6d}
.badge-neutral{background:rgba(255,184,0,0.2);color:#ffb800}
.historical-header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;margin-bottom:20px}
.historical-title{color:#00e5ff;font-size:1.2rem}
.hamburger{display:none;font-size:28px;cursor:pointer;color:#00e5ff;position:fixed;top:15px;left:20px;z-index:1100;background:#121826;padding:8px 12px;border-radius:8px;border:1px solid #2a3040}
/* Force sidebar collapse width */
.sidebar.collapsed {
    width: 80px !important;
    min-width: 80px !important;
}
.sidebar.collapsed ~ .main-content {
    margin-left: 80px !important;
}
@media (max-width:768px){
    .sidebar{transform:translateX(-100%);width:260px !important}
    .sidebar.open{transform:translateX(0)}
    .sidebar.collapsed{width:260px !important; min-width:260px !important}
    .sidebar.collapsed .logo-text{opacity:1;visibility:visible}
    .sidebar.collapsed .menu-item span:not(.menu-icon){display:inline-block;opacity:1}
    .sidebar.collapsed .menu-icon{margin-right:12px}
    .main-content{margin-left:0 !important;padding:60px 15px 20px 15px !important;width:100%}
    .sidebar.collapsed ~ .main-content { margin-left: 0 !important; }
    .hamburger{display:block}
    .navbar{flex-direction:column;align-items:flex-start;gap:10px;padding:10px 15px}
    th,td{padding:8px 4px;font-size:12px}
    .historical-header{flex-direction:column;align-items:flex-start;gap:10px}
    .dropdown-selector{margin-left:0;width:100%}
}
</style>
</head>
<body>
<div class="hamburger" onclick="toggleSidebar()">☰</div>
<div class="sidebar" id="sidebar">
    <div class="logo">
        <span class="logo-text">⚡ Tradion</span>
        <button class="sidebar-toggle" onclick="toggleSidebarCollapse()">◀</button>
    </div>
    <div class="menu-item" onclick="window.location.href='/dashboard'"><i data-lucide="chart-line" class="menu-icon"></i><span>Dashboard</span></div>
    <div class="menu-item active" onclick="window.location.href='/currencies'"><i data-lucide="currency" class="menu-icon"></i><span>COT Data</span></div>
    <div class="menu-item" onclick="window.location.href='/scorecard'"><i data-lucide="trending-up" class="menu-icon"></i><span>Asset Scorecard</span></div>
    <div class="menu-item" onclick="window.location.href='/sentiment'"><i data-lucide="message-circle" class="menu-icon"></i><span>Sentiment</span></div>
    <div class="menu-item" onclick="window.location.href='/heatmap'"><i data-lucide="flame" class="menu-icon"></i><span>Heatmap</span></div>
    {% if is_admin %}<div class="menu-item" onclick="window.location.href='/admin'"><i data-lucide="crown" class="menu-icon"></i><span>Admin</span></div>{% endif %}
    <div class="menu-item" onclick="window.location.href='/profile'"><i data-lucide="user" class="menu-icon"></i><span>Profile</span></div>
    <div class="menu-item" onclick="logout()"><i data-lucide="log-out" class="menu-icon"></i><span>Logout</span></div>
</div>
<div class="main-content" id="mainContent">
    <div class="navbar"><div class="navbar-title">COT Data · Economic Sentiment & Non‑Commercial Positions</div><div><button onclick="loadCurrencies()" class="secondary"><i data-lucide="refresh-cw" style="width:16px;height:16px;margin-right:6px"></i> Refresh</button></div></div>
    <div id="loading" style="display:none"><div class="loading-skeleton" style="height:200px;border-radius:12px"></div></div>
    <div id="currencyTable"></div>
    <div class="chart-container"><h3><i data-lucide="bar-chart-2" style="width:18px;height:18px;margin-right:6px"></i> Non‑Commercial Longs vs Shorts (% of Total)</h3><canvas id="cotBarChart" width="800" height="400"></canvas></div>
    
    <!-- HISTORICAL COT SECTION -->
    <div class="historical-section">
        <div class="historical-header">
            <div class="historical-title"><i data-lucide="trending-up" style="width:18px;height:18px;margin-right:6px"></i> Historical COT Net Positions</div>
            <div>
                <select id="historyCurrencySelect" class="dropdown-selector" onchange="loadHistoricalCOT()">
                    <option value="">Select currency...</option>
                    <option value="USD">USD</option>
                    <option value="EUR">EUR</option>
                    <option value="GBP">GBP</option>
                    <option value="JPY">JPY</option>
                    <option value="AUD">AUD</option>
                    <option value="NZD">NZD</option>
                    <option value="CAD">CAD</option>
                    <option value="CHF">CHF</option>
                    <option value="XAU">XAU</option>
                    <option value="BTC">BTC</option>
                </select>
            </div>
        </div>
        <div id="historicalInfo" style="margin-bottom:15px; display:flex; align-items:center; gap:15px; flex-wrap:wrap"></div>
        <div class="chart-container" style="margin-top:0">
            <canvas id="historyChart" width="800" height="400"></canvas>
        </div>
        <div id="historyLoading" style="display:none; text-align:center; padding:20px">Loading historical data...</div>
    </div>
</div>
<script>
let currentCurrencies = [];
let barChart = null;
let historyChart = null;
let currentHistoryData = { dates: [], netPositions: [], weeklyChanges: [] };

function toggleSidebar() {
    document.getElementById('sidebar').classList.toggle('open');
}
function toggleSidebarCollapse() {
    const sidebar = document.getElementById('sidebar');
    if (window.innerWidth > 768) {
        sidebar.classList.toggle('collapsed');
        localStorage.setItem('sidebarCollapsed', sidebar.classList.contains('collapsed'));
    }
}
function restoreSidebarState() {
    const sidebar = document.getElementById('sidebar');
    if (window.innerWidth > 768) {
        const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
        if (isCollapsed) sidebar.classList.add('collapsed');
    }
}
window.addEventListener('resize', function() {
    const sidebar = document.getElementById('sidebar');
    if (window.innerWidth <= 768) {
        sidebar.classList.remove('collapsed');
    } else {
        const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
        if (isCollapsed) sidebar.classList.add('collapsed');
    }
});
async function loadCurrencies(){
    document.getElementById('loading').style.display='block';
    document.getElementById('currencyTable').innerHTML='';
    try{
        const res=await fetch('/api/currencies');
        if(!res.ok) throw new Error('Failed to load');
        const data=await res.json();
        currentCurrencies = data.currencies;
        displayCurrencies(currentCurrencies);
        renderBarChart(currentCurrencies);
    }catch(e){
        document.getElementById('currencyTable').innerHTML='<div style="color:#ff4d6d;padding:20px">❌ Error loading currency data</div>';
    }finally{
        document.getElementById('loading').style.display='none';
    }
}
function displayCurrencies(currencies){
    let html='<table><thead><tr><th>Currency</th><th>Economic Sentiment</th><th>COT Net Position</th><th>COT Weekly Change</th><th>Longs (Contracts)</th><th>Shorts (Contracts)</th></tr></thead><tbody>';
    currencies.forEach(c=>{
        const econColor=c.econ_pct>=55?'#00e5a0':(c.econ_pct<=45?'#ff4d6d':'#ffb800');
        const netClass=c.cot_net>=0?'positive':'negative';
        const changeClass=c.cot_change>=0?'positive':'negative';
        html+=`<tr><td style="font-weight:bold;font-size:1.1em">${c.currency}</td>
                <td><div style="display:flex;align-items:center;justify-content:center;gap:8px"><span style="color:${econColor};font-weight:bold">${c.econ_pct.toFixed(1)}%</span><div class="gauge-bar"><div class="gauge-fill" style="width:${c.econ_pct}%;background:${econColor}"></div></div></div></td>
                <td class="${netClass}">${c.cot_net.toLocaleString()}</td>
                <td class="${changeClass}">${c.cot_change.toLocaleString()}</td>
                <td>${c.longs.toLocaleString()}</td>
                <td>${c.shorts.toLocaleString()}</td>
            </tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('currencyTable').innerHTML=html;
}
function renderBarChart(currencies){
    const ctx=document.getElementById('cotBarChart').getContext('2d');
    const labels=currencies.map(c=>c.currency);
    const longPcts=currencies.map(c=>c.long_pct);
    const shortPcts=currencies.map(c=>c.short_pct);
    if(barChart) barChart.destroy();
    barChart=new Chart(ctx,{
        type:'bar',
        data:{labels:labels,datasets:[{label:'Long %',data:longPcts,backgroundColor:'rgba(0,229,160,0.7)',borderColor:'#00e5a0',borderWidth:1},{label:'Short %',data:shortPcts,backgroundColor:'rgba(255,77,109,0.7)',borderColor:'#ff4d6d',borderWidth:1}]},
        options:{responsive:true,maintainAspectRatio:true,scales:{x:{title:{display:true,text:'Currency',color:'#a0b0c0'},ticks:{color:'#fff'}},y:{title:{display:true,text:'Percentage (%)',color:'#a0b0c0'},ticks:{color:'#fff',beginAtZero:true,max:100}}},plugins:{legend:{labels:{color:'#fff'},position:'top'},tooltip:{callbacks:{label:function(context){return `${context.dataset.label}: ${context.raw.toFixed(1)}%`}}}}}
    });
}
async function loadHistoricalCOT() {
    const currency = document.getElementById('historyCurrencySelect').value;
    if (!currency) return;
    const loadingDiv = document.getElementById('historyLoading');
    const infoDiv = document.getElementById('historicalInfo');
    loadingDiv.style.display = 'block';
    infoDiv.innerHTML = '';
    try {
        const res = await fetch(`/api/cot/history/${currency}`);
        if (!res.ok) throw new Error('Failed to load history');
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        currentHistoryData = {
            dates: data.dates || [],
            netPositions: data.net_positions || [],
            weeklyChanges: data.weekly_changes || []
        };
        renderHistoryChart(currentHistoryData);
        displayHistoricalInfo(data);
    } catch (err) {
        infoDiv.innerHTML = `<span style="color:#ff4d6d">❌ ${err.message}</span>`;
        if (historyChart) historyChart.destroy();
    } finally {
        loadingDiv.style.display = 'none';
    }
}
function renderHistoryChart(history) {
    const ctx = document.getElementById('historyChart').getContext('2d');
    if (historyChart) historyChart.destroy();
    if (!history.dates.length) {
        document.getElementById('historicalInfo').innerHTML = '<span style="color:#ffb800">No historical data available for this currency yet.</span>';
        return;
    }
    historyChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: history.dates,
            datasets: [
                {
                    label: 'Net Positions',
                    data: history.netPositions,
                    borderColor: '#00e5ff',
                    backgroundColor: 'rgba(0,229,255,0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 3,
                    pointBackgroundColor: '#00e5ff'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            scales: {
                x: {
                    ticks: { color: '#a0b0c0', maxRotation: 45, autoSkip: true },
                    title: { display: true, text: 'Report Date', color: '#a0b0c0' }
                },
                y: {
                    ticks: { color: '#a0b0c0' },
                    title: { display: true, text: 'Net Positions (Contracts)', color: '#a0b0c0' }
                }
            },
            plugins: {
                legend: { labels: { color: '#fff' } },
                tooltip: { callbacks: { label: (ctx) => `${ctx.dataset.label}: ${ctx.raw.toLocaleString()}` } }
            }
        }
    });
}
function displayHistoricalInfo(data) {
    const infoDiv = document.getElementById('historicalInfo');
    if (!data.bias && !data.trend) {
        infoDiv.innerHTML = '';
        return;
    }
    let biasHtml = '';
    if (data.bias) {
        const biasClass = data.bias === 'Bullish' ? 'badge-bullish' : (data.bias === 'Bearish' ? 'badge-bearish' : 'badge-neutral');
        biasHtml = `<span class="badge ${biasClass}">Current Bias: ${data.bias}</span>`;
    }
    let trendHtml = '';
    if (data.trend) {
        const trendClass = data.trend === 'Bullish' ? 'badge-bullish' : (data.trend === 'Bearish' ? 'badge-bearish' : 'badge-neutral');
        trendHtml = `<span class="badge ${trendClass}">Trend: ${data.trend}</span>`;
    }
    infoDiv.innerHTML = `${biasHtml} ${trendHtml}`;
}
function logout(){fetch('/logout').then(()=>window.location.href='/login');}
restoreSidebarState();
loadCurrencies();

// Initialize Lucide icons
setTimeout(() => {
    if (typeof lucide !== 'undefined') lucide.createIcons();
}, 100);
</script>
</body>
</html>''')

                # Sentiment (with Lucide icons)
        with open('templates/sentiment.html', 'w') as f:
            f.write('''<!DOCTYPE html>
<html><head><title>Sentiment – Tradion</title><meta name="viewport" content="width=device-width, initial-scale=1.0">
<script src="https://unpkg.com/lucide@latest"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI',sans-serif;background:#0B0F1A;color:#fff;display:flex}
.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100}
.sidebar .logo{font-size:22px;color:#00e5ff;text-align:center;margin-bottom:30px}
.sidebar .menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:8px;cursor:pointer;color:#a0b0c0}
.sidebar .menu-item:hover{background:rgba(0,229,255,0.1);color:#00e5ff}
.sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A}
.sidebar .menu-icon{font-size:20px;margin-right:12px}
.sidebar .menu-icon svg{width:20px;height:20px;vertical-align:middle}
.main-content{flex:1;margin-left:280px;padding:20px 30px}
.header h2{color:#00e5ff;margin-bottom:20px}
.search-box{padding:10px 15px;background:#1a1f2e;border:1px solid #2a3040;border-radius:8px;color:#fff;width:100%;max-width:350px;margin-bottom:20px}
.sentiment-table{width:100%;border-collapse:collapse;background:#121826;border-radius:12px;overflow:hidden}
.sentiment-table th,.sentiment-table td{padding:12px 15px;border-bottom:1px solid #2a3040;text-align:left}
.sentiment-table th{background:rgba(0,229,255,0.1);color:#00e5ff}
.bar-container{display:flex;align-items:center;gap:10px}
.bar-wrapper{flex:1;height:24px;background:#1a1f2e;border-radius:12px;overflow:hidden;display:flex}
.bar-long{background:#00e5a0;height:100%;display:flex;align-items:center;justify-content:center;color:#0B0F1A;font-weight:bold;font-size:0.8rem;transition:width 0.3s}
.bar-short{background:#ff4d6d;height:100%;display:flex;align-items:center;justify-content:center;color:#fff;font-weight:bold;font-size:0.8rem;transition:width 0.3s}
.percentage-col{width:80px;text-align:right}
.pair-col{font-weight:bold;width:120px}
/* ===== Mobile / Responsive Styles ===== */
.hamburger {
    display: none;
    font-size: 28px;
    cursor: pointer;
    color: #00e5ff;
    position: fixed;
    top: 15px;
    left: 20px;
    z-index: 1100;
    background: #121826;
    padding: 8px 12px;
    border-radius: 8px;
    border: 1px solid #2a3040;
}

@media (max-width: 768px) {
    .sidebar {
        transform: translateX(-100%);
        transition: transform 0.3s ease;
        width: 260px;
        z-index: 1050;
        position: fixed;
        top: 0;
        left: 0;
        height: 100%;
        background: #121826;
    }
    .sidebar.open {
        transform: translateX(0);
    }
    .main-content {
        margin-left: 0 !important;
        padding: 60px 15px 20px 15px !important;
        width: 100%;
    }
    .hamburger {
        display: block;
    }
    .navbar {
        flex-direction: column;
        align-items: flex-start;
        gap: 10px;
        padding: 10px 15px;
    }
    table, .currency-grid, .gauge-panel, .scorecard-grid {
        font-size: 12px;
    }
    th, td {
        padding: 8px 4px;
    }
    .currency-card {
        padding: 12px;
    }
    .gauge-wrapper {
        width: 90px;
        height: 90px;
    }
    .gauge-value {
        font-size: 18px;
    }
    .scorecard-grid {
        grid-template-columns: 1fr;
        gap: 15px;
    }
    .panel {
        padding: 12px;
    }
    .cot-data-table th, .cot-data-table td {
        padding: 8px 6px;
        font-size: 12px;
    }
    .inline-edit {
        width: 80px;
        padding: 4px 6px;
        font-size: 0.75rem;
    }
}
</style>
</head>
<body>
<div class="hamburger" onclick="toggleSidebar()">☰</div>
<div class="sidebar">
    <div class="logo">⚡ Tradion</div>
    <div class="menu-item" onclick="window.location.href='/dashboard'"><i data-lucide="chart-line" class="menu-icon"></i><span>Dashboard</span></div>
    <div class="menu-item" onclick="window.location.href='/currencies'"><i data-lucide="currency" class="menu-icon"></i><span>COT Data</span></div>
    <div class="menu-item" onclick="window.location.href='/scorecard'"><i data-lucide="trending-up" class="menu-icon"></i><span>Scorecard</span></div>
    <div class="menu-item active" onclick="window.location.href='/sentiment'"><i data-lucide="message-circle" class="menu-icon"></i><span>Sentiment</span></div>
    <div class="menu-item" onclick="window.location.href='/profile'"><i data-lucide="user" class="menu-icon"></i><span>Profile</span></div>
    <div class="menu-item" onclick="logout()"><i data-lucide="log-out" class="menu-icon"></i><span>Logout</span></div>
</div>
<div class="main-content">
    <div class="header"><h2>Market Sentiment</h2></div>
    <input type="text" id="searchSentiment" class="search-box" placeholder="Search pair...">
    <table class="sentiment-table" id="sentimentTable"><thead><tr><th>Pair</th><th>Sentiment Bar</th><th>Long %</th><th>Short %</th></tr></thead><tbody></tbody></table>
</div>
<script>
// Toggle mobile sidebar
function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    sidebar.classList.toggle('open');
}

// Close sidebar when clicking outside (optional)
document.addEventListener('click', function(event) {
    const sidebar = document.getElementById('sidebar');
    const hamburger = document.querySelector('.hamburger');
    if (sidebar && hamburger && !sidebar.contains(event.target) && !hamburger.contains(event.target) && sidebar.classList.contains('open')) {
        sidebar.classList.remove('open');
    }
});
async function loadSentiment(){const res=await fetch('/api/sentiment');const data=await res.json();renderSentiment(data)}
function renderSentiment(data){const tbody=document.querySelector('#sentimentTable tbody');tbody.innerHTML='';data.forEach(s=>{const long=s.long_pct,short=s.short_pct;const row=document.createElement('tr');row.innerHTML=`<td class="pair-col">${s.pair}</td><td><div class="bar-wrapper"><div class="bar-long" style="width:${long}%">${long>0?long.toFixed(0)+'%':''}</div><div class="bar-short" style="width:${short}%">${short>0?short.toFixed(0)+'%':''}</div></div></td><td class="percentage-col" style="color:#00e5a0">${long}%</td><td class="percentage-col" style="color:#ff4d6d">${short}%</td>`;tbody.appendChild(row)})}
document.getElementById('searchSentiment').addEventListener('input',function(){const term=this.value.toLowerCase();const rows=document.querySelectorAll('#sentimentTable tbody tr');rows.forEach(row=>{const pair=row.querySelector('.pair-col').textContent.toLowerCase();row.style.display=pair.includes(term)?'':'none'})});
function logout(){fetch('/logout').then(()=>window.location.href='/login')}
loadSentiment();

// Initialise Lucide icons after page loads
setTimeout(() => {
    if (typeof lucide !== 'undefined') lucide.createIcons();
}, 100);
</script>
</body>
</html>''')

               # Heatmap (with gauges, caching, and Lucide icons)
    with open('templates/heatmap.html', 'w') as f:
        f.write('''<!DOCTYPE html>
<html><head><title>Currency Heatmap · Tradion</title><meta name="viewport" content="width=device-width, initial-scale=1.0">
<script src="https://unpkg.com/lucide@latest"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;display:flex}
.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100;transition:width 0.3s ease, transform 0.3s ease;overflow-x:hidden;white-space:nowrap}
.sidebar .logo{display:flex;justify-content:space-between;align-items:center;font-size:22px;font-weight:800;color:#00e5ff;margin-bottom:25px;padding-bottom:15px;border-bottom:1px solid #2a3040}
.sidebar .logo-text{transition:opacity 0.2s}
.sidebar.collapsed .logo-text{opacity:0;width:0;visibility:hidden}
.sidebar-toggle{background:none;border:none;color:#00e5ff;font-size:18px;cursor:pointer;padding:4px 8px;border-radius:6px;transition:0.2s}
.sidebar-toggle:hover{background:rgba(0,229,255,0.2)}
.sidebar .menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:10px;cursor:pointer;transition:all 0.2s;color:#a0b0c0}
.sidebar .menu-item:hover{background:rgba(0,229,255,0.08);color:#00e5ff}
.sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;font-weight:bold}
.sidebar .menu-icon{font-size:20px;margin-right:12px;transition:margin 0.2s}
.sidebar .menu-icon svg{width:20px;height:20px;vertical-align:middle}
.sidebar.collapsed .menu-icon{margin-right:0}
.sidebar .menu-item span:not(.menu-icon){transition:opacity 0.2s}
.sidebar.collapsed .menu-item span:not(.menu-icon){opacity:0;width:0;display:none}
.main-content{flex:1;margin-left:280px;padding:20px 30px;transition:margin-left 0.3s}
.navbar{background:#121826;padding:15px 25px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #2a3040;border-radius:0 0 16px 16px;margin-bottom:20px}
.navbar-title{font-size:18px;color:#00e5ff}
button{padding:10px 20px;background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;border:none;border-radius:8px;cursor:pointer;font-weight:bold;transition:all 0.2s}
button:hover{transform:scale(1.02);box-shadow:0 0 15px rgba(0,229,255,0.3)}
.heatmap-grid{display:grid;grid-template-columns:repeat(auto-fill, minmax(180px, 1fr));gap:20px;margin-top:20px}
.heatmap-card{background:#121826;border-radius:16px;padding:16px;text-align:center;border:1px solid #2a3040;transition:transform 0.2s;position:relative;border-top:3px solid transparent}
.heatmap-card:hover{transform:translateY(-5px);border-color:#00e5ff}
.currency-code{font-size:1.4rem;font-weight:bold;margin-bottom:8px}
.gauge-wrapper{position:relative;width:100px;height:100px;margin:10px auto}
.gauge-svg{transform:rotate(-90deg);width:100%;height:100%}
.gauge-bg-circle{stroke:#2a3040;stroke-width:8;fill:none}
.gauge-fill-circle{stroke-width:8;fill:none;stroke-linecap:round;transition:stroke-dasharray 0.8s}
.gauge-center{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center}
.gauge-value{font-size:20px;font-weight:bold;color:#00e5ff}
.strength-value{font-size:1.6rem;font-weight:800;margin:8px 0}
.econ-pct{font-size:0.8rem;color:#8892b0;margin-top:5px}
.loading{text-align:center;padding:40px}
.hamburger{display:none;font-size:28px;cursor:pointer;color:#00e5ff;position:fixed;top:15px;left:20px;z-index:1100;background:#121826;padding:8px 12px;border-radius:8px;border:1px solid #2a3040}
.sidebar.collapsed{width:80px !important;min-width:80px !important}
.sidebar.collapsed ~ .main-content{margin-left:80px !important}
@media (max-width:768px){
    .sidebar{transform:translateX(-100%);width:260px !important}
    .sidebar.open{transform:translateX(0)}
    .sidebar.collapsed{width:260px !important; min-width:260px !important}
    .sidebar.collapsed .logo-text{opacity:1;visibility:visible}
    .sidebar.collapsed .menu-item span:not(.menu-icon){display:inline-block;opacity:1}
    .sidebar.collapsed .menu-icon{margin-right:12px}
    .main-content{margin-left:0 !important;padding:60px 15px 20px 15px !important;width:100%}
    .sidebar.collapsed ~ .main-content{margin-left:0 !important}
    .hamburger{display:block}
    .heatmap-grid{grid-template-columns:repeat(auto-fill, minmax(140px, 1fr));gap:12px}
    .gauge-wrapper{width:80px;height:80px}
    .gauge-value{font-size:16px}
}
</style>
</head>
<body>
<div class="hamburger" onclick="toggleSidebar()">☰</div>
<div class="sidebar" id="sidebar">
    <div class="logo">
        <span class="logo-text">⚡ Tradion</span>
        <button class="sidebar-toggle" onclick="toggleSidebarCollapse()">◀</button>
    </div>
    <div class="menu-item" onclick="window.location.href='/dashboard'"><i data-lucide="chart-line" class="menu-icon"></i><span>Dashboard</span></div>
    <div class="menu-item" onclick="window.location.href='/currencies'"><i data-lucide="currency" class="menu-icon"></i><span>COT Data</span></div>
    <div class="menu-item" onclick="window.location.href='/scorecard'"><i data-lucide="trending-up" class="menu-icon"></i><span>Asset Scorecard</span></div>
    <div class="menu-item" onclick="window.location.href='/sentiment'"><i data-lucide="message-circle" class="menu-icon"></i><span>Sentiment</span></div>
    <div class="menu-item active" onclick="window.location.href='/heatmap'"><i data-lucide="flame" class="menu-icon"></i><span>Heatmap</span></div>
    {% if is_admin %}<div class="menu-item" onclick="window.location.href='/admin'"><i data-lucide="crown" class="menu-icon"></i><span>Admin</span></div>{% endif %}
    <div class="menu-item" onclick="window.location.href='/profile'"><i data-lucide="user" class="menu-icon"></i><span>Profile</span></div>
    <div class="menu-item" onclick="logout()"><i data-lucide="log-out" class="menu-icon"></i><span>Logout</span></div>
</div>
<div class="main-content">
    <div class="navbar">
        <div class="navbar-title">Currency Heatmap · Strength & Economic Gauge</div>
        <div>
            <button onclick="refreshHeatmap()"><i data-lucide="refresh-cw" style="width:16px;height:16px;margin-right:6px"></i> Refresh</button>
            <span id="lastUpdateTime" style="margin-left:15px; font-size:12px; color:#8892b0"></span>
        </div>
    </div>
    <div id="heatmapContainer" class="heatmap-grid"></div>
</div>
<script>
const heatmapCacheKey = 'tradion_heatmap';
const heatmapTimeKey = 'tradion_heatmap_time';
const cacheTTL = 5 * 60 * 1000; // 5 minutes

function toggleSidebar() {
    document.getElementById('sidebar').classList.toggle('open');
}
function toggleSidebarCollapse() {
    const sidebar = document.getElementById('sidebar');
    if (window.innerWidth > 768) {
        sidebar.classList.toggle('collapsed');
        localStorage.setItem('sidebarCollapsed', sidebar.classList.contains('collapsed'));
    }
}
function restoreSidebarState() {
    const sidebar = document.getElementById('sidebar');
    if (window.innerWidth > 768) {
        const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
        if (isCollapsed) sidebar.classList.add('collapsed');
    }
}
window.addEventListener('resize', function() {
    const sidebar = document.getElementById('sidebar');
    if (window.innerWidth <= 768) {
        sidebar.classList.remove('collapsed');
    } else {
        const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
        if (isCollapsed) sidebar.classList.add('collapsed');
    }
});

async function loadHeatmap(forceRefresh = false) {
    if (!forceRefresh) {
        const cachedData = sessionStorage.getItem(heatmapCacheKey);
        const cachedTime = sessionStorage.getItem(heatmapTimeKey);
        if (cachedData && cachedTime && (Date.now() - parseInt(cachedTime)) < cacheTTL) {
            try {
                const data = JSON.parse(cachedData);
                renderHeatmap(data);
                document.getElementById('lastUpdateTime').innerHTML = 'Cached: ' + new Date(parseInt(cachedTime)).toLocaleTimeString();
                return;
            } catch(e) { console.warn('Cache parse error', e); }
        }
    }

    const container = document.getElementById('heatmapContainer');
    container.innerHTML = '<div class="loading">Loading currency strength...</div>';
    try {
        const res = await fetch('/api/currency_strength');
        if (!res.ok) throw new Error('Failed to load');
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        renderHeatmap(data.currencies);
        const now = Date.now();
        sessionStorage.setItem(heatmapCacheKey, JSON.stringify(data.currencies));
        sessionStorage.setItem(heatmapTimeKey, now.toString());
        document.getElementById('lastUpdateTime').innerHTML = 'Updated: ' + new Date().toLocaleTimeString();
    } catch(e) {
        container.innerHTML = `<div class="loading" style="color:#ff4d6d">❌ ${e.message}</div>`;
    }
}

function renderHeatmap(currencies) {
    const container = document.getElementById('heatmapContainer');
    container.innerHTML = '';
    currencies.forEach(curr => {
        const strength = curr.strength;
        const econPct = curr.econ_pct;
        const circumference = 2 * Math.PI * 40; // radius 40
        const dashOffset = circumference * (1 - econPct / 100);
        let gaugeColor = econPct >= 70 ? '#00e5a0' : (econPct >= 55 ? '#66ffb3' : (econPct >= 45 ? '#ffb800' : (econPct >= 30 ? '#ff8099' : '#ff4d6d')));
        let strengthColor = strength > 0 ? '#00e5a0' : (strength < 0 ? '#ff4d6d' : '#ffb800');
        let borderColor = strength > 0 ? '#00e5a0' : (strength < 0 ? '#ff4d6d' : '#ffb800');
        const card = document.createElement('div');
        card.className = 'heatmap-card';
        card.style.borderTopColor = borderColor;
        card.innerHTML = `
            <div class="currency-code">${curr.currency}</div>
            <div class="gauge-wrapper">
                <svg class="gauge-svg" viewBox="0 0 100 100">
                    <circle class="gauge-bg-circle" cx="50" cy="50" r="40"></circle>
                    <circle class="gauge-fill-circle" cx="50" cy="50" r="40" style="stroke: ${gaugeColor}; stroke-dasharray: ${circumference}; stroke-dashoffset: ${dashOffset}"></circle>
                </svg>
                <div class="gauge-center">
                    <div class="gauge-value">${econPct.toFixed(1)}%</div>
                </div>
            </div>
            <div class="strength-value" style="color:${strengthColor}">${strength > 0 ? '+' : ''}${strength}</div>
            <div class="econ-pct">Economic Sentiment</div>
        `;
        container.appendChild(card);
    });
}

function refreshHeatmap() {
    sessionStorage.removeItem(heatmapCacheKey);
    sessionStorage.removeItem(heatmapTimeKey);
    loadHeatmap(true);
}

function logout() { fetch('/logout').then(() => window.location.href = '/login'); }

restoreSidebarState();
loadHeatmap();

// Initialise Lucide icons after page loads and after dynamic updates
setTimeout(() => {
    if (typeof lucide !== 'undefined') lucide.createIcons();
}, 100);
</script>
</body>
</html>''')

    
# -----------------------------
# MAIN
# -----------------------------
if __name__ == '__main__':
    create_templates()
    
    # Start background scheduler for retail sentiment (Myfxbook)
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from datetime import timedelta
    import atexit
    
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=update_retail_sentiment, trigger="interval", minutes=30, id='retail_sentiment_job')
    
    # Schedule score history saving every 3 days at midnight UTC
    def save_all_asset_scores():
        with app.app_context():
            # Save scores for individual currencies
            for currency in MAJOR_CURRENCIES:
                try:
                    score = get_asset_score(currency)
                    save_asset_score_history(currency, score)
                    print(f"Saved score for {currency}: {score}")
                except Exception as e:
                    print(f"Error saving score for {currency}: {e}")
            
            # Save scores for all forex pairs
            for base, quote in ALL_PAIRS:
                pair = f"{base}/{quote}"
                try:
                    weighted = calculate_weighted_pair_score(pair, base, quote)
                    total_score = weighted["total_raw"]
                    save_asset_score_history(pair, total_score)
                    print(f"Saved score for {pair}: {total_score}")
                except Exception as e:
                    print(f"Error saving score for {pair}: {e}")
    
    # Add the job (runs every 3 days at 00:00 UTC)
    scheduler.add_job(
        func=save_all_asset_scores,
        trigger=CronTrigger(day='*/3', hour=0, minute=0),
        id='score_history_job'
    )
    
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
    
    def open_browser():
        time.sleep(2)
        webbrowser.open('http://localhost:5000')
    
    threading.Thread(target=open_browser, daemon=True).start()
    
    print("=" * 50)
    print("Tradion - Dark Pro Trader Edition")
    print("Admin: Karlmax / admin4125")
    print("All requested changes implemented:")
    print("- Sentiment column inverted (contrarian)")
    print("- COT admin: longs, shorts, auto net position")
    print("- Currencies page renamed to 'COT Data' with vertical bar graph")
    print("- Asset Scorecard padding increased (medium size)")
    print("- Technical score now uses 21-day SMA from Yahoo Finance")
    print("- Retail sentiment auto-fetched from Myfxbook (every 30 minutes)")
    print("- Score history saved every 3 days at midnight UTC (max 30 entries)")
    print("=" * 50)
    
    app.run(debug=False, use_reloader=False, host='0.0.0.0', port=5000)