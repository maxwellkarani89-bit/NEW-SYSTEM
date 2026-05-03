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
# -------- PASTE THE DATABASE CONFIGURATION HERE --------
import os

database_url = os.environ.get('DATABASE_URL')
if database_url:
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://', 1)
    app.config['SQLALCHEMY_DATABASE_URI'] = database_url
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

    # Create default admin if missing
    admin = User.query.filter_by(username='Karlmax').first()
    if not admin:
        admin = User(username='Karlmax', email='maxwellkarani89@gmail.com',
                     is_admin=True, is_active=True)
        admin.set_password('admin4125')
        db.session.add(admin)
        db.session.commit()

    # Create default admin if missing
    admin = User.query.filter_by(username='Karlmax').first()
    if not admin:
        admin = User(username='Karlmax', email='maxwellkarani89@gmail.com',
                     is_admin=True, is_active=True)
        admin.set_password('admin4125')
        db.session.add(admin)
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
    "XAU/USD": "XAUUSD=X",
    "BTC/USD": "BTC-USD",
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
        
        lower_better = any(k.lower() in name.lower() for k in LOWER_BETTER_INDICATORS)
        
        # Determine if indicator is bullish or bearish normally
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
    # Normal calculation
    base_pct = analyze_currency_econ(base)
    quote_pct = analyze_currency_econ(quote)
    base_comp = calculate_currency_composite_score(base)
    quote_comp = calculate_currency_composite_score(quote)
    combined_diff = (base_comp - quote_comp)

    # Invert if pair is XAU/USD (gold moves opposite to USD)
    if (base == "XAU" and quote == "USD") or (base == "BTC" and quote == "USD"):
        combined_diff = -combined_diff   # flip the polarity

    abs_diff = abs(combined_diff)
    if abs_diff > 6: econ_score = 4
    elif abs_diff > 3: econ_score = 3
    elif abs_diff > 1.5: econ_score = 2
    elif abs_diff > 0: econ_score = 1
    else: econ_score = 0
    if combined_diff < 0: econ_score = -econ_score

    if combined_diff > 6: bias, sym = "EXTREME BULLISH", "▲▲▲"
    elif combined_diff > 3: bias, sym = "VERY BULLISH", "▲▲"
    elif combined_diff > 1.5: bias, sym = "BULLISH", "▲"
    elif combined_diff > 0: bias, sym = "SLIGHTLY BULLISH", "▲"
    elif combined_diff > -1.5: bias, sym = "SLIGHTLY BEARISH", "▼"
    elif combined_diff > -3: bias, sym = "BEARISH", "▼"
    elif combined_diff > -6: bias, sym = "VERY BEARISH", "▼▼"
    else: bias, sym = "EXTREME BEARISH", "▼▼▼"

    return bias, sym, combined_diff, econ_score

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

        sentiment_data = {s.pair: s for s in SentimentData.query.all()}

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
            econ_data = {'bias': str(econ_bias), 'symbol': str(econ_sym), 'diff': float(diff_pct), 'score': int(econ_score)}

            yf_symbol = SYMBOL_MAPPING.get(pair_name, pair_name.replace('/', '') + '=X')
            trend_score = get_trend_score(yf_symbol)
            trend_bias = "Bullish" if trend_score > 0 else "Bearish" if trend_score < 0 else "Neutral"
            trend_symbol = "▲" if trend_score > 0 else "▼" if trend_score < 0 else "●"

            seasonality_bias, seasonality_score = get_seasonality_bias(pair_name)

            # Sentiment score & label (contrarian: many shorts => bullish)
            sent = sentiment_data.get(pair_name)
            if sent:
                long_p = sent.long_pct
                short_p = sent.short_pct
                if short_p > long_p:
                    sent_score = 2
                    sentiment_label = "Bullish"
                elif long_p > short_p:
                    sent_score = -2
                    sentiment_label = "Bearish"
                else:
                    sent_score = 0
                    sentiment_label = "Neutral"
            else:
                sentiment_label = "N/A"
                sent_score = 0

            overall_score = cot_score + momentum_score + econ_score + trend_score + seasonality_score + sent_score
            overall_bias, overall_symbol, overall_color, display_score = get_overall_bias_and_color(overall_score)

            result = {
                'pair': str(pair_name),
                'cot': cot_data,
                'economic': econ_data,
                'trend': {'bias': trend_bias, 'symbol': trend_symbol, 'score': trend_score},
                'seasonality': {'bias': seasonality_bias, 'score': seasonality_score},
                'sentiment': {'bias': sentiment_label, 'score': sent_score},
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
    try:
        base, quote = pair.split('/')
        base_cot_rec = COTData.query.filter_by(currency=base.upper()).first()
        quote_cot_rec = COTData.query.filter_by(currency=quote.upper()).first()
        base_cot = {"net": base_cot_rec.net_position if base_cot_rec else 0,
                    "change": base_cot_rec.weekly_change if base_cot_rec else 0}
        quote_cot = {"net": quote_cot_rec.net_position if quote_cot_rec else 0,
                     "change": quote_cot_rec.weekly_change if quote_cot_rec else 0}
        base_econ = analyze_currency_econ(base)
        quote_econ = analyze_currency_econ(quote)
        base_comp = calculate_currency_composite_score(base)
        quote_comp = calculate_currency_composite_score(quote)
        base_indicators = EconomicIndicator.query.filter_by(currency=base.upper()).all()
        quote_indicators = EconomicIndicator.query.filter_by(currency=quote.upper()).all()
        return jsonify({
            'pair': pair,
            'base': {
                'currency': base,
                'cot_net': base_cot['net'],
                'cot_change': base_cot['change'],
                'econ_pct': round(base_econ, 1),
                'composite_score': base_comp,
                'indicators': [{
                    'name': i.indicator_name,
                    'forecast': i.forecast,
                    'actual': i.actual,
                    'is_lower_better': i.is_lower_better,
                    'category': i.category or 'General'
                } for i in base_indicators]
            },
            'quote': {
                'currency': quote,
                'cot_net': quote_cot['net'],
                'cot_change': quote_cot['change'],
                'econ_pct': round(quote_econ, 1),
                'composite_score': quote_comp,
                'indicators': [{
                    'name': i.indicator_name,
                    'forecast': i.forecast,
                    'actual': i.actual,
                    'is_lower_better': i.is_lower_better,
                    'category': i.category or 'General'
                } for i in quote_indicators]
            }
        })
    except Exception as e:
        return jsonify({'error': 'Unable to load pair details'}), 500

@app.route('/api/asset_scorecard/<symbol>')
@login_required
def api_asset_scorecard(symbol):
    parts = symbol.split('/')
    if len(parts) == 2:
        base, quote = parts[0].upper(), parts[1].upper()
    else:
        base = symbol.upper()
        quote = None

    yf_symbol = SYMBOL_MAPPING.get(symbol, symbol.replace('/', '') + '=X')
    # Use 21-day SMA for technical score
    technical_score = get_trend_score_21d(yf_symbol)

    base_cot = COTData.query.filter_by(currency=base).first()
    quote_cot = COTData.query.filter_by(currency=quote).first() if quote else None
    cot_net = (base_cot.net_position if base_cot else 0) - (quote_cot.net_position if quote_cot else 0) if quote else (base_cot.net_position if base_cot else 0)
    cot_change = (base_cot.weekly_change if base_cot else 0) - (quote_cot.weekly_change if quote_cot else 0) if quote else (base_cot.weekly_change if base_cot else 0)
    cot_score = 1 if cot_net > 0 else (-1 if cot_net < 0 else 0)
    momentum_score = 1 if cot_change > 0 else (-1 if cot_change < 0 else 0)
    sentiment_cot_score = cot_score + momentum_score

    base_indicators = EconomicIndicator.query.filter_by(currency=base).all()
    category_data = {}
    for ind in base_indicators:
        cat = ind.category or 'General'
        surprise = ind.actual - ind.forecast
        if ind.is_lower_better:
            surprise = -surprise
        score = 1 if surprise > 0 else (-1 if surprise < 0 else 0)
        category_data.setdefault(cat, []).append(score)

    category_bias = {}
    for cat, scores in category_data.items():
        if scores:
            avg = sum(scores) / len(scores)
            category_bias[cat] = round(avg * len(scores), 1)

    fundamentals_score = round(sum(v for k,v in category_bias.items() if k != 'Technical Bias'), 1)

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
        'base_indicators': [{
            'name': i.indicator_name,
            'forecast': i.forecast,
            'actual': i.actual,
            'is_lower_better': i.is_lower_better,
            'category': i.category or 'General'
        } for i in base_indicators],
        'quote_indicators': [] if not quote else [{
            'name': i.indicator_name,
            'forecast': i.forecast,
            'actual': i.actual,
            'is_lower_better': i.is_lower_better,
            'category': i.category or 'General'
        } for i in EconomicIndicator.query.filter_by(currency=quote.upper()).all()],
        'seasonality_bias': season_bias,
        'seasonality_score': season_score,
        'sentiment_score': sent_score_val,
        'score_history': [0,1,2,1,3,2,4,3,2,1,2,0]
    })

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

@app.route('/admin/cot_data', methods=['POST'])
@login_required
@admin_required
def admin_cot_data_update():
    data = request.json
    currency = data['currency'].upper()
    cot = COTData.query.filter_by(currency=currency).first()
    if not cot:
        cot = COTData(currency=currency)
        db.session.add(cot)
    cot.longs = float(data['longs'])
    cot.shorts = float(data['shorts'])
    cot.net_position = cot.longs - cot.shorts          # auto-compute
    cot.weekly_change = float(data['weekly_change'])
    db.session.commit()
    return jsonify({'success': True})

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

@app.route('/sentiment')
@login_required
def sentiment_page():
    return render_template('sentiment.html', username=session['username'], is_admin=session.get('is_admin', False))

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

    # Dashboard
    with open('templates/dashboard.html', 'w') as f:
        f.write('''<!DOCTYPE html>
<html><head><title>Tradion · Dashboard</title><meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;display:flex}.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100;transition:width 0.3s}.sidebar.collapsed{width:80px}.sidebar .logo{font-size:24px;font-weight:800;color:#00e5ff;text-align:center;margin-bottom:30px;padding-bottom:20px;border-bottom:1px solid #2a3040;display:flex;align-items:center;justify-content:center}.sidebar.collapsed .logo{font-size:20px}.sidebar.collapsed .logo span{display:none}.sidebar .menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:10px;cursor:pointer;transition:all 0.2s;color:#a0b0c0}.sidebar .menu-item:hover{background:rgba(0,229,255,0.08);color:#00e5ff}.sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;font-weight:bold}.sidebar .menu-icon{font-size:20px;margin-right:12px}.sidebar.collapsed .menu-icon{margin-right:0}.sidebar.collapsed .menu-item span:not(.menu-icon){display:none}.main-content{flex:1;margin-left:280px;padding:20px 30px;transition:margin-left 0.3s}.navbar{background:#121826;padding:15px 25px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #2a3040;border-radius:0 0 16px 16px;margin-bottom:20px}.navbar-title{font-size:18px;color:#00e5ff}button{padding:10px 20px;background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;border:none;border-radius:8px;cursor:pointer;font-weight:bold;transition:all 0.2s}button:hover{transform:scale(1.02);box-shadow:0 0 15px rgba(0,229,255,0.3)}button.secondary{background:transparent;border:1px solid #00e5ff;color:#00e5ff}table{width:100%;border-collapse:collapse;margin-top:20px;background:#121826;font-size:13px;border-radius:12px;overflow:hidden}th,td{padding:12px 8px;text-align:center;border-bottom:1px solid #2a3040}th{background:rgba(0,229,255,0.1);color:#00e5ff;font-weight:600}.currency-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:20px;margin-top:20px}.currency-card{background:#121826;border-radius:16px;padding:20px 15px;text-align:center;transition:all 0.2s;border:1px solid #2a3040;cursor:pointer;position:relative}.currency-card:hover{transform:translateY(-5px);border-color:#00e5ff;box-shadow:0 10px 25px rgba(0,229,255,0.1)}.favorite-star{position:absolute;top:10px;right:10px;font-size:20px;cursor:pointer;color:#ffb800;opacity:0.6;transition:all 0.2s}.favorite-star.active{opacity:1;text-shadow:0 0 10px #ffb800}.gauge-wrapper{position:relative;width:120px;height:120px;margin:15px auto}.gauge-svg{transform:rotate(-90deg);width:100%;height:100%}.gauge-bg-circle{stroke:#2a3040;stroke-width:10;fill:none}.gauge-fill-circle{stroke-width:10;fill:none;stroke-linecap:round;transition:stroke-dasharray 0.8s}.gauge-center{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center}.gauge-value{font-size:22px;font-weight:bold;color:#00e5ff}.content-pane{display:none}.content-pane.active{display:block}.modal{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.7);z-index:1000;justify-content:center;align-items:center}.modal-content{background:#121826;padding:25px;border-radius:16px;width:90%;max-width:600px;border:1px solid #2a3040;max-height:80vh;overflow-y:auto}.loading-skeleton{background:linear-gradient(90deg,#1a1f2e,#2a3040,#1a1f2e);background-size:200% 100%;animation:shimmer 1.5s infinite}@keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}.search-box{width:100%;max-width:300px;padding:10px 15px;background:#1a1f2e;border:1px solid #2a3040;border-radius:8px;color:#fff;margin-bottom:15px}.clickable-pair{cursor:pointer;text-decoration:underline dotted #00e5ff}.detail-row{display:flex;justify-content:space-between;margin-bottom:8px}.toggle-btn{background:none;border:none;color:#00e5ff;font-size:24px;cursor:pointer;margin-right:10px}.indicator-table{width:100%;margin-top:10px;background:#1a1f2e;border-radius:8px}.indicator-table th,.indicator-table td{padding:8px;text-align:left;border-bottom:1px solid #2a3040;font-size:0.9rem}.indicator-table th{background:rgba(0,229,255,0.1);color:#00e5ff}</style>
</head>
<body>
<div class="sidebar" id="sidebar">
    <div class="logo"><button class="toggle-btn" onclick="toggleSidebar()">☰</button><span>⚡ Tradion</span></div>
    <div class="menu-item active" onclick="showPane('analysis')"><span class="menu-icon">📊</span><span>Analysis</span></div>
    <div class="menu-item" onclick="showPane('heatmap')"><span class="menu-icon">🔥</span><span>Heatmap</span></div>
    <div class="menu-item" onclick="window.location.href='/currencies'"><span class="menu-icon">💱</span><span>COT Data</span></div>
    <div class="menu-item" onclick="window.location.href='/scorecard'"><span class="menu-icon">📈</span><span>Asset Scorecard</span></div>
    <div class="menu-item" onclick="window.location.href='/sentiment'"><span class="menu-icon">💬</span><span>Sentiment</span></div>
    <div class="menu-item" onclick="showPane('history')"><span class="menu-icon">📜</span><span>History</span></div>
    {% if is_admin %}<div class="menu-item" onclick="window.location.href='/admin'"><span class="menu-icon">👑</span><span>Admin</span></div>{% endif %}
    <div class="menu-item" onclick="window.location.href='/profile'"><span class="menu-icon">👤</span><span>Profile</span></div>
    <div class="menu-item" onclick="logout()"><span class="menu-icon">🚪</span><span>Logout</span></div>
</div>
<div class="main-content" id="mainContent">
    <div class="navbar"><div class="navbar-title">Welcome, {{ username }}! {% if is_admin %}<span style="background:#ffb800;padding:4px 12px;border-radius:20px;font-size:12px;color:#000">👑 ADMIN</span>{% endif %}</div><div><span id="lastUpdateTime" style="font-size:12px;color:#8892b0"></span></div></div>
    <div id="analysisPane" class="content-pane active">
        <div style="margin-bottom:20px;display:flex;gap:10px;align-items:center;flex-wrap:wrap"><button onclick="loadAnalysis()">🔥 RUN ANALYSIS</button><button onclick="refreshAnalysis()" class="secondary">🔄 Force Refresh</button><input type="text" id="searchInput" class="search-box" placeholder="🔍 Search pair..."></div>
        <div id="loading" style="display:none"><div class="loading-skeleton" style="height:200px;border-radius:12px"></div></div>
        <div id="results"></div>
    </div>
    <div id="heatmapPane" class="content-pane"><h2 style="color:#00e5ff;margin-bottom:15px">Currency Strength & Economic Gauge</h2><div id="heatmapContent"></div></div>
    <div id="historyPane" class="content-pane"><button onclick="loadHistory()" class="secondary">Load History</button><div id="historyContent" style="margin-top:20px"></div></div>
</div>
<!-- Pair Detail Modal -->
<div class="modal" id="detailModal"><div class="modal-content"><h3 style="color:#00e5ff;margin-bottom:20px" id="modalPairTitle">EUR/USD Details</h3><div id="modalDetailContent">Loading...</div><div style="margin-top:20px;text-align:right"><button onclick="closeDetailModal()" class="secondary">Close</button></div></div></div>
<script>
let currentData=null,heatmapData=null,favorites={{ favorites|safe }},currentResults=[];
function toggleSidebar(){const sidebar=document.getElementById('sidebar');const main=document.getElementById('mainContent');sidebar.classList.toggle('collapsed');if(sidebar.classList.contains('collapsed')){main.style.marginLeft='80px'}else{main.style.marginLeft='280px'}}
function showPane(pane){document.querySelectorAll('.content-pane').forEach(p=>p.classList.remove('active'));document.getElementById(pane+'Pane').classList.add('active');document.querySelectorAll('.menu-item').forEach(item=>{item.classList.remove('active');const onclick=item.getAttribute('onclick');if(onclick&&onclick.includes(`'${pane}'`)){item.classList.add('active')}});if(pane==='history') loadHistory()}
async function loadAnalysis(){document.getElementById('loading').style.display='block';try{const res=await fetch('/api/analyze',{method:'POST'});if(!res.ok) throw new Error((await res.json()).error);const data=await res.json();currentData=data.results;heatmapData=data.heatmap;currentResults=data.results;displayResults(data.results);displayHeatmap(data.heatmap);document.getElementById('lastUpdateTime').innerHTML='Updated: '+new Date().toLocaleTimeString()}catch(e){document.getElementById('results').innerHTML='<div style="color:#ff4d6d;padding:20px">❌ '+e.message+'</div>'}finally{document.getElementById('loading').style.display='none'}}
async function refreshAnalysis(){await fetch('/admin/refresh',{method:'POST'});loadAnalysis()}
function filterResults(){const term=document.getElementById('searchInput').value.toLowerCase();const filtered=currentResults.filter(r=>r.pair.toLowerCase().includes(term));displayResults(filtered)}
function displayResults(data){let html='<div style="overflow-x:auto"><table><thead><th>Pair</th><th>COT</th><th>Momentum</th><th>Economic</th><th>Trend</th><th>Seasonality</th><th>Sentiment</th><th>Overall</th></thead><tbody>';data.forEach(item=>{const isFav=favorites.includes(item.pair);html+=`<tr><td style="font-weight:bold"><span class="clickable-pair" onclick="showPairDetail('${item.pair}')">${item.pair}</span><span class="favorite-star ${isFav?'active':''}" onclick="toggleFavorite('${item.pair}',this);event.stopPropagation();">⭐</span></td><td style="color:${item.cot.bias==='Bullish'?'#00e5a0':(item.cot.bias==='Bearish'?'#ff4d6d':'#ffb800')}">${item.cot.symbol} ${item.cot.bias}<br><small>[${item.cot.score}]</small></td><td style="color:${item.cot.momentum==='Bullish'?'#00e5a0':(item.cot.momentum==='Bearish'?'#ff4d6d':'#ffb800')}">${item.cot.mom_symbol} ${item.cot.momentum}<br><small>[${item.cot.momentum_score}]</small></td><td style="color:${item.economic.bias.includes('BULLISH')?'#00e5a0':(item.economic.bias.includes('BEARISH')?'#ff4d6d':'#ffb800')}">${item.economic.symbol}<br><small>[${item.economic.score}]</small></td><td style="color:${item.trend.bias==='Bullish'?'#00e5a0':(item.trend.bias==='Bearish'?'#ff4d6d':'#ffb800')}">${item.trend.symbol}<br><small>[${item.trend.score}]</small></td><td>${item.seasonality.bias}<br><small>[${item.seasonality.score}]</small></td><td style="color:${(item.sentiment&&item.sentiment.bias==='Bullish')?'#00e5a0':((item.sentiment&&item.sentiment.bias==='Bearish')?'#ff4d6d':'#ffb800')}">${item.sentiment?item.sentiment.bias:'N/A'}<br><small>[${item.sentiment?item.sentiment.score:'0'}]</small></td><td style="color:${item.overall.color};font-weight:bold">${item.overall.symbol} ${item.overall.bias}<br><strong>[${item.overall.score}]</strong></td></tr>`});html+='</tbody></table></div>';document.getElementById('results').innerHTML=html;document.getElementById('searchInput').addEventListener('input',filterResults)}
function displayHeatmap(heatmap){if(!heatmap) return;let html='<div class="currency-grid">';heatmap.ranking.forEach(curr=>{let currency=curr[0],score=curr[1],econPct=heatmap.econ_data[currency]||50;let scoreColor=score>3?'#00e5a0':(score>1.5?'#66ffb3':(score>0.5?'#99ffcc':(score>-0.5?'#ffb800':(score>-1.5?'#ffb3c1':(score>-3?'#ff8099':'#ff4d6d')))));let circumference=2*Math.PI*50;let dashOffset=circumference*(1-econPct/100);let gaugeColor=econPct>=70?'#00e5a0':(econPct>=55?'#66ffb3':(econPct>=45?'#ffb800':(econPct>=30?'#ff8099':'#ff4d6d')));html+=`<div class="currency-card" style="border-top:3px solid ${scoreColor}" onclick="showCurrencyDetail('${currency}')"><strong style="color:${scoreColor}">${currency}</strong><div class="gauge-wrapper"><svg class="gauge-svg" viewBox="0 0 120 120"><circle class="gauge-bg-circle" cx="60" cy="60" r="50"></circle><circle class="gauge-fill-circle" cx="60" cy="60" r="50" style="stroke:${gaugeColor};stroke-dasharray:${circumference};stroke-dashoffset:${dashOffset}"></circle></svg><div class="gauge-center"><div class="gauge-value">${econPct.toFixed(1)}%</div></div></div><div>Score: ${score.toFixed(2)}</div></div>`});html+='</div>';document.getElementById('heatmapContent').innerHTML=html}
function toggleFavorite(pair,el){const idx=favorites.indexOf(pair);if(idx>-1) favorites.splice(idx,1);else favorites.push(pair);fetch('/api/favorites',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({favorites})});el.classList.toggle('active')}
async function showPairDetail(pair){document.getElementById('modalPairTitle').innerText=pair+' Details';document.getElementById('modalDetailContent').innerHTML='<div class="loading-skeleton" style="height:150px"></div>';document.getElementById('detailModal').style.display='flex';try{const res=await fetch('/api/pair_detail/'+encodeURIComponent(pair));const data=await res.json();let html=`<div style="background:#1a1f2e;padding:15px;border-radius:8px;margin-bottom:15px"><h4 style="color:#00e5ff">${data.base.currency}</h4><div class="detail-row"><span>COT Net Position:</span><span style="color:${data.base.cot_net>=0?'#00e5a0':'#ff4d6d'}">${data.base.cot_net.toLocaleString()}</span></div><div class="detail-row"><span>COT Weekly Change:</span><span style="color:${data.base.cot_change>=0?'#00e5a0':'#ff4d6d'}">${data.base.cot_change.toLocaleString()}</span></div><div class="detail-row"><span>Economic Sentiment:</span><span>${data.base.econ_pct.toFixed(1)}%</span></div>${buildIndicatorTable(data.base.indicators,'base')}</div><div style="background:#1a1f2e;padding:15px;border-radius:8px"><h4 style="color:#00e5ff">${data.quote.currency}</h4><div class="detail-row"><span>COT Net Position:</span><span style="color:${data.quote.cot_net>=0?'#00e5a0':'#ff4d6d'}">${data.quote.cot_net.toLocaleString()}</span></div><div class="detail-row"><span>COT Weekly Change:</span><span style="color:${data.quote.cot_change>=0?'#00e5a0':'#ff4d6d'}">${data.quote.cot_change.toLocaleString()}</span></div><div class="detail-row"><span>Economic Sentiment:</span><span>${data.quote.econ_pct.toFixed(1)}%</span></div>${buildIndicatorTable(data.quote.indicators,'quote')}</div>`;document.getElementById('modalDetailContent').innerHTML=html}catch(e){document.getElementById('modalDetailContent').innerHTML='<p style="color:#ff4d6d">Error loading details</p>'}}
function buildIndicatorTable(indicators,section){if(!indicators||indicators.length===0) return '<p style="margin-top:10px;color:#8892b0">No economic indicators.</p>';let html='<table class="indicator-table"><thead><tr><th>Indicator</th><th>Forecast</th><th>Actual</th><th>Lower Better?</th></tr></thead><tbody>';indicators.forEach(ind=>{const color=ind.actual>ind.forecast?(ind.is_lower_better?'#ff4d6d':'#00e5a0'):(ind.actual<ind.forecast?(ind.is_lower_better?'#00e5a0':'#ff4d6d'):'#ffb800');html+=`<tr><td>${ind.name}</td><td>${ind.forecast}</td><td style="color:${color};font-weight:bold">${ind.actual}</td><td>${ind.is_lower_better?'Yes':'No'}</td></tr>`});html+='</tbody></table>';return html}
function closeDetailModal(){document.getElementById('detailModal').style.display='none'}
async function loadHistory(){const r=await fetch('/api/history');const d=await r.json();let html='<table><thead><tr><th>Date</th><th>Pair</th><th>COT</th><th>Economic</th><th>Trend</th><th>Seasonality</th><th>Overall</th></tr></thead><tbody>';d.forEach(i=>{html+=`<tr><td>${i.date}</td><td>${i.pair}</td><td>${i.cot_bias}</td><td>${i.econ_bias}</td><td>${i.trend_score}</td><td>${i.seasonality_bias||'N/A'}</td><td>${i.overall_bias}</td></tr>`});html+='</tbody></table>';document.getElementById('historyContent').innerHTML=html}
function logout(){fetch('/logout').then(()=>window.location.href='/login')}
loadAnalysis();
</script>
</body>
</html>''')

    # Scorecard (with increased padding)
    with open('templates/scorecard.html', 'w') as f:
        f.write('''<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Asset Scorecard – Tradion</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;display:flex}.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100;transition:width 0.3s}.sidebar.collapsed{width:80px}.sidebar .logo{font-size:20px;font-weight:800;color:#00e5ff;text-align:center;margin-bottom:20px;padding-bottom:15px;border-bottom:1px solid #2a3040;display:flex;align-items:center;justify-content:center}.sidebar.collapsed .logo span{display:none}.sidebar .menu-item{display:flex;align-items:center;padding:10px 12px;margin:3px 0;border-radius:8px;cursor:pointer;transition:all 0.2s;color:#a0b0c0}.sidebar .menu-item:hover{background:rgba(0,229,255,0.08);color:#00e5ff}.sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;font-weight:bold}.sidebar .menu-icon{font-size:18px;margin-right:10px}.sidebar.collapsed .menu-icon{margin-right:0}.sidebar.collapsed .menu-item span:not(.menu-icon){display:none}.main-content{flex:1;margin-left:280px;padding:25px 35px;transition:margin-left 0.3s;max-width:1200px}.header{display:flex;justify-content:space-between;align-items:center;margin-bottom:25px}.header h2{color:#00e5ff;font-size:2rem}.symbol-selector select{padding:10px 14px;background:#121826;border:1.5px solid #2a3040;color:#fff;border-radius:8px;font-size:1rem;min-width:200px}.scorecard-grid{display:grid;grid-template-columns:1fr 1fr;gap:25px}.gauge-panel{background:#121826;border-radius:16px;padding:25px;display:flex;flex-direction:column;align-items:center;border:1px solid #2a3040;position:relative}.gauge-container{position:relative;width:200px;height:100px;overflow:hidden}.gauge-svg{width:100%;height:100%}.gauge-bg{stroke:#2a3040;stroke-width:12;fill:none}.gauge-fill{stroke-width:12;fill:none;stroke-linecap:round;transition:stroke-dasharray 0.5s}.gauge-needle-group{transition:transform 0.5s ease-out}.needle-body{stroke:#fff;stroke-width:2;fill:none}.needle-tip{fill:#fff;stroke:#fff;stroke-width:1}.gauge-center{position:absolute;bottom:0;left:50%;transform:translateX(-50%);text-align:center}.gauge-label{font-size:1.4rem;font-weight:bold;color:#00e5ff}.gauge-bias{font-size:1rem;color:#00e5a0}.loading-overlay{position:absolute;top:0;left:0;right:0;bottom:0;background:rgba(18,24,38,0.9);display:flex;align-items:center;justify-content:center;border-radius:16px;z-index:10}.loading-overlay.hidden{display:none}.spinner{border:4px solid #2a3040;border-top:4px solid #00e5ff;border-radius:50%;width:36px;height:36px;animation:spin 1s linear infinite}@keyframes spin{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}.score-summary{display:flex;flex-direction:column;gap:10px;width:100%;margin-top:20px}.score-item{display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:#1a1f2e;border-radius:10px;font-size:1rem}.score-label{font-weight:600}.score-value{font-weight:bold;padding:3px 10px;border-radius:20px;font-size:0.9rem}.score-value.positive{background:rgba(0,229,160,0.2);color:#00e5a0}.score-value.negative{background:rgba(255,77,109,0.2);color:#ff4d6d}.score-value.neutral{background:rgba(255,184,0,0.2);color:#ffb800}.trend-chart{display:flex;align-items:flex-end;gap:6px;height:60px;margin-top:15px}.trend-bar{flex:1;background:#2a3040;border-radius:4px 4px 0 0;min-width:8px;transition:height 0.3s}.trend-bar.positive{background:#00e5a0}.trend-bar.negative{background:#ff4d6d}.panel{background:#121826;border-radius:14px;padding:18px;border:1px solid #2a3040;margin-bottom:16px;font-size:0.95rem}.panel h3{color:#00e5ff;font-size:1.1rem;margin-bottom:12px;border-bottom:1px solid #2a3040;padding-bottom:10px}.indicator-row{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid rgba(42,48,64,0.5);font-size:0.9rem}.indicator-row:last-child{border-bottom:none}.indicator-label{font-size:0.9rem}.indicator-values{display:flex;gap:15px}.value{font-weight:500;width:70px;text-align:right}.value.positive{color:#00e5a0}.value.negative{color:#ff4d6d}.value.neutral{color:#a0b0c0}.surprise{font-size:0.75rem;padding:2px 6px;border-radius:12px}.surprise.positive{background:rgba(0,229,160,0.2);color:#00e5a0}.surprise.negative{background:rgba(255,77,109,0.2);color:#ff4d6d}.toggle-btn{background:none;border:none;color:#00e5ff;font-size:22px;cursor:pointer;margin-right:8px}</style>
</head>
<body>
<div class="sidebar" id="sidebar"><div class="logo"><button class="toggle-btn" onclick="toggleSidebar()">☰</button><span>⚡ Tradion</span></div><div class="menu-item" onclick="window.location.href='/dashboard'"><span class="menu-icon">📊</span><span>Dashboard</span></div><div class="menu-item" onclick="window.location.href='/currencies'"><span class="menu-icon">💱</span><span>COT Data</span></div><div class="menu-item active"><span class="menu-icon">📈</span><span>Asset Scorecard</span></div><div class="menu-item" onclick="window.location.href='/sentiment'"><span class="menu-icon">💬</span><span>Sentiment</span></div><div class="menu-item" onclick="window.location.href='/profile'"><span class="menu-icon">👤</span><span>Profile</span></div><div class="menu-item" onclick="logout()"><span class="menu-icon">🚪</span><span>Logout</span></div></div>
<div class="main-content" id="mainContent">
    <div class="header"><h2>Asset Scorecard</h2><div class="symbol-selector"><select id="symbolSelect" onchange="loadScorecard()"><option value="">Select Currency...</option></select></div></div>
    <div class="scorecard-grid">
        <div class="gauge-panel" id="gaugePanel">
            <div class="loading-overlay" id="loadingOverlay"><div class="spinner"></div></div>
            <div class="gauge-container">
                <svg class="gauge-svg" viewBox="0 0 220 110">
                    <defs><linearGradient id="gaugeGradient" x1="0%" y1="0%" x2="100%" y2="0%"><stop offset="0%" stop-color="#ff4d6d"/><stop offset="50%" stop-color="#ffb800"/><stop offset="100%" stop-color="#00e5a0"/></linearGradient></defs>
                    <path class="gauge-bg" d="M20,110 A90,90 0 0,1 200,110"/>
                    <path id="gaugeFill" class="gauge-fill" d="M20,110 A90,90 0 0,1 200,110" stroke="url(#gaugeGradient)"/>
                    <g id="gaugeNeedleGroup" class="gauge-needle-group" transform="rotate(-90,110,110)"><line class="needle-body" x1="110" y1="110" x2="110" y2="30"/><circle class="needle-tip" cx="110" cy="30" r="4"/></g>
                </svg>
            </div>
            <div class="gauge-center"><div class="gauge-label" id="gaugeBias">Neutral</div><div class="gauge-bias" id="gaugeValue">0.0</div></div>
            <div class="score-summary">
                <div class="score-item"><span class="score-label">Tradion Score</span><span id="tradionScore" class="score-value neutral">0</span></div>
                <div class="score-item"><span class="score-label">Technical (21d SMA)</span><span id="technicalScore" class="score-value neutral">● 0</span></div>
                <div class="score-item"><span class="score-label">Sentiment + COT</span><span id="sentimentCOTScore" class="score-value neutral">0</span></div>
                <div class="score-item"><span class="score-label">Fundamentals</span><span id="fundamentalsScore" class="score-value neutral">0</span></div>
            </div>
            <div class="trend-chart" id="trendChart"></div>
        </div>
        <div id="categoryCards"></div>
    </div>
</div>
<script>
const currencies={{ currencies|tojson }};
const select=document.getElementById('symbolSelect');
currencies.forEach(cur=>{const opt=document.createElement('option');opt.value=cur;opt.textContent=cur;select.appendChild(opt)});
function toggleSidebar(){const sidebar=document.getElementById('sidebar');const main=document.getElementById('mainContent');sidebar.classList.toggle('collapsed');if(sidebar.classList.contains('collapsed')){main.style.marginLeft='80px'}else{main.style.marginLeft='280px'}}
const CATEGORIES=["Technical Bias","Economic Growth Bias","Inflation Bias","Jobs Market Bias","Crowd Sentiment (COT)"];
async function loadScorecard(){const symbol=select.value;if(!symbol) return;const overlay=document.getElementById('loadingOverlay');overlay.classList.remove('hidden');try{const res=await fetch('/api/asset_scorecard/'+encodeURIComponent(symbol));const data=await res.json();updateGauge(data.overall.score,data.overall.bias,data.overall.color);updateScores(data);updateTrendChart(data.score_history);renderCategoryCards(data)}catch(e){console.error(e)}finally{overlay.classList.add('hidden')}}
function updateGauge(score,bias,color){const clampedScore=Math.min(Math.max(score,-10),10);const angle=((clampedScore+10)/20)*180;document.getElementById('gaugeNeedleGroup').setAttribute('transform',`rotate(${angle-90},110,110)`);document.getElementById('gaugeBias').textContent=bias;document.getElementById('gaugeValue').textContent=score.toFixed(1);document.querySelector('.gauge-bias').style.color=color}
function updateScores(data){setScoreValue('tradionScore',data.tradion_score);setScoreValue('technicalScore',data.technical_score>0?'▲ 1':(data.technical_score<0?'▼ -1':'● 0'));setScoreValue('sentimentCOTScore',data.sentiment_cot_score);setScoreValue('fundamentalsScore',data.fundamentals_score,data.fundamentals_score>0?'positive':(data.fundamentals_score<0?'negative':'neutral'))}
function setScoreValue(id,text,classNameOverride){const el=document.getElementById(id);el.textContent=text;if(classNameOverride){el.className='score-value '+classNameOverride}else{const val=parseFloat(text);el.className='score-value '+(val>0?'positive':val<0?'negative':'neutral')}}
function updateTrendChart(history){const container=document.getElementById('trendChart');container.innerHTML='';history.forEach(val=>{const bar=document.createElement('div');bar.className='trend-bar '+(val>=0?'positive':'negative');bar.style.height=Math.abs(val)*5+'px';container.appendChild(bar)})}
function renderCategoryCards(data) {
    const container = document.getElementById('categoryCards');
    container.innerHTML = '';

    // --- Technical Bias card (special: include 21-day SMA score) ---
    const techCard = document.createElement('div');
    techCard.className = 'panel';
    let techHtml = `<h3>Technical Bias</h3>`;
    // Add the 21-day SMA technical score row
    const techScore = data.technical_score;
    const techDirection = techScore > 0 ? 'Bullish ▲' : (techScore < 0 ? 'Bearish ▼' : 'Neutral ●');
    const techColor = techScore > 0 ? 'positive' : (techScore < 0 ? 'negative' : 'neutral');
    techHtml += `
        <div class="indicator-row" style="font-weight:bold;margin-bottom:6px">
            <span>21-day SMA Trend</span>
            <span class="value ${techColor}">${techDirection} (${techScore > 0 ? '+' : ''}${techScore})</span>
        </div>
    `;
    // Then show the regular Technical Bias indicators (if any)
    const techIndicators = data.base_indicators.filter(ind => ind.category === 'Technical Bias');
    if (techIndicators.length === 0) {
        techHtml += '<p style="color:#8892b0; margin-top:8px">No economic indicators in Technical Bias.</p>';
    } else {
        techIndicators.forEach(ind => {
            const surprise = ind.actual - ind.forecast;
            const better = ind.is_lower_better ? surprise < 0 : surprise > 0;
            const colorClass = better ? 'positive' : (surprise === 0 ? 'neutral' : 'negative');
            techHtml += `
                <div class="indicator-row">
                    <span class="indicator-label">${ind.name}</span>
                    <div class="indicator-values">
                        <span class="value">Actual: ${ind.actual}</span>
                        <span class="value">Forecast: ${ind.forecast}</span>
                        <span class="surprise ${colorClass}">${surprise.toFixed(1)}</span>
                    </div>
                </div>
            `;
        });
    }
    techCard.innerHTML = techHtml;
    container.appendChild(techCard);

    // --- Other categories (unchanged, but skip Technical Bias because we already added it) ---
    const otherCategories = ["Economic Growth Bias", "Inflation Bias", "Jobs Market Bias", "Crowd Sentiment (COT)"];
    otherCategories.forEach(category => {
        const indicators = data.base_indicators.filter(ind => ind.category === category);
        const biasScore = data.category_bias[category] || 0;
        const card = document.createElement('div');
        card.className = 'panel';
        let html = `<h3>${category}</h3>`;
        html += `<div class="indicator-row" style="font-weight:bold;margin-bottom:6px">
                    <span>Bias Score</span>
                    <span class="value ${biasScore > 0 ? 'positive' : (biasScore < 0 ? 'negative' : 'neutral')}">
                        ${biasScore > 0 ? '+' : ''}${biasScore}
                    </span>
                </div>`;
        if (indicators.length === 0) {
            html += '<p style="color:#8892b0">No indicators in this category.</p>';
        } else {
            indicators.forEach(ind => {
                const surprise = ind.actual - ind.forecast;
                const better = ind.is_lower_better ? surprise < 0 : surprise > 0;
                const colorClass = better ? 'positive' : (surprise === 0 ? 'neutral' : 'negative');
                html += `
                    <div class="indicator-row">
                        <span class="indicator-label">${ind.name}</span>
                        <div class="indicator-values">
                            <span class="value">Actual: ${ind.actual}</span>
                            <span class="value">Forecast: ${ind.forecast}</span>
                            <span class="surprise ${colorClass}">${surprise.toFixed(1)}</span>
                        </div>
                    </div>
                `;
            });
        }
        card.innerHTML = html;
        container.appendChild(card);
    });
}function logout(){fetch('/logout').then(()=>window.location.href='/login')}
if(currencies.length>0){select.value=currencies[0];loadScorecard()}
</script>
</body>
</html>''')

    # Currencies (COT Data with bar chart)
    with open('templates/currencies.html', 'w') as f:
        f.write('''<!DOCTYPE html>
<html><head><title>COT Data - Tradion</title><meta name="viewport" content="width=device-width, initial-scale=1.0">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI','Inter',sans-serif;background:#0B0F1A;color:#E0E0E0;display:flex}.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100;transition:width 0.3s}.sidebar.collapsed{width:80px}.sidebar .logo{font-size:22px;font-weight:800;color:#00e5ff;text-align:center;margin-bottom:25px;padding-bottom:15px;border-bottom:1px solid #2a3040;display:flex;align-items:center;justify-content:center}.sidebar.collapsed .logo{font-size:18px}.sidebar.collapsed .logo span{display:none}.sidebar .menu-item{display:flex;align-items:center;padding:12px 15px;margin:5px 0;border-radius:10px;cursor:pointer;transition:all 0.2s;color:#a0b0c0}.sidebar .menu-item:hover{background:rgba(0,229,255,0.08);color:#00e5ff}.sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;font-weight:bold}.sidebar .menu-icon{font-size:20px;margin-right:12px}.sidebar.collapsed .menu-icon{margin-right:0}.sidebar.collapsed .menu-item span:not(.menu-icon){display:none}.main-content{flex:1;margin-left:280px;padding:20px 30px;transition:margin-left 0.3s}.navbar{background:#121826;padding:15px 25px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #2a3040;border-radius:0 0 16px 16px;margin-bottom:20px}.navbar-title{font-size:18px;color:#00e5ff}button{padding:10px 20px;background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A;border:none;border-radius:8px;cursor:pointer;font-weight:bold;transition:all 0.2s}button:hover{transform:scale(1.02);box-shadow:0 0 15px rgba(0,229,255,0.3)}button.secondary{background:transparent;border:1px solid #00e5ff;color:#00e5ff}table{width:100%;border-collapse:collapse;margin-top:20px;background:#121826;font-size:14px;border-radius:12px;overflow:hidden}th,td{padding:14px 12px;text-align:center;border-bottom:1px solid #2a3040}th{background:rgba(0,229,255,0.1);color:#00e5ff;font-weight:600}.gauge-bar{width:100px;height:8px;background:#2a3040;border-radius:4px;overflow:hidden;margin:0 auto}.gauge-fill{height:100%;border-radius:4px;transition:width 0.3s}.positive{color:#00e5a0}.negative{color:#ff4d6d}.neutral{color:#ffb800}.loading-skeleton{background:linear-gradient(90deg,#1a1f2e,#2a3040,#1a1f2e);background-size:200% 100%;animation:shimmer 1.5s infinite}@keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}.toggle-btn{background:none;border:none;color:#00e5ff;font-size:22px;cursor:pointer;margin-right:8px}.chart-container{background:#121826;border-radius:16px;padding:20px;margin-top:30px;border:1px solid #2a3040}.chart-container h3{color:#00e5ff;margin-bottom:15px;font-size:1.2rem}canvas{max-height:400px;width:100%}</style>
</head>
<body>
<div class="sidebar" id="sidebar"><div class="logo"><button class="toggle-btn" onclick="toggleSidebar()">☰</button><span>⚡ Tradion</span></div><div class="menu-item" onclick="window.location.href='/dashboard'"><div class="menu-icon">📊</div><span>Dashboard</span></div><div class="menu-item active" onclick="window.location.href='/currencies'"><div class="menu-icon">💱</div><span>COT Data</span></div><div class="menu-item" onclick="window.location.href='/scorecard'"><span class="menu-icon">📈</span><span>Asset Scorecard</span></div><div class="menu-item" onclick="window.location.href='/sentiment'"><span class="menu-icon">💬</span><span>Sentiment</span></div>{% if is_admin %}<div class="menu-item" onclick="window.location.href='/admin'"><div class="menu-icon">👑</div><span>Admin</span></div>{% endif %}<div class="menu-item" onclick="window.location.href='/profile'"><div class="menu-icon">👤</div><span>Profile</span></div><div class="menu-item" onclick="logout()"><div class="menu-icon">🚪</div><span>Logout</span></div></div>
<div class="main-content" id="mainContent">
    <div class="navbar"><div class="navbar-title">COT Data · Economic Sentiment & Non‑Commercial Positions</div><div><button onclick="loadCurrencies()" class="secondary">🔄 Refresh</button></div></div>
    <div id="loading" style="display:none"><div class="loading-skeleton" style="height:200px;border-radius:12px"></div></div>
    <div id="currencyTable"></div>
    <div class="chart-container"><h3>📊 Non‑Commercial Longs vs Shorts (% of Total)</h3><canvas id="cotBarChart" width="800" height="400"></canvas></div>
</div>
<script>
let currentCurrencies = [];
function toggleSidebar(){const sidebar=document.getElementById('sidebar');const main=document.getElementById('mainContent');sidebar.classList.toggle('collapsed');if(sidebar.classList.contains('collapsed')){main.style.marginLeft='80px'}else{main.style.marginLeft='280px'}}
async function loadCurrencies(){document.getElementById('loading').style.display='block';document.getElementById('currencyTable').innerHTML='';try{const res=await fetch('/api/currencies');if(!res.ok) throw new Error('Failed to load');const data=await res.json();currentCurrencies = data.currencies;displayCurrencies(currentCurrencies);renderBarChart(currentCurrencies);}catch(e){document.getElementById('currencyTable').innerHTML='<div style="color:#ff4d6d;padding:20px">❌ Error loading currency data</div>'}finally{document.getElementById('loading').style.display='none'}}
function displayCurrencies(currencies){let html='<table><thead><tr><th>Currency</th><th>Economic Sentiment</th><th>COT Net Position</th><th>COT Weekly Change</th><th>Longs (Contracts)</th><th>Shorts (Contracts)</th></tr></thead><tbody>';currencies.forEach(c=>{const econColor=c.econ_pct>=55?'#00e5a0':(c.econ_pct<=45?'#ff4d6d':'#ffb800');const netClass=c.cot_net>=0?'positive':'negative';const changeClass=c.cot_change>=0?'positive':'negative';html+=`<tr><td style="font-weight:bold;font-size:1.1em">${c.currency}</td><td><div style="display:flex;align-items:center;justify-content:center;gap:8px"><span style="color:${econColor};font-weight:bold">${c.econ_pct.toFixed(1)}%</span><div class="gauge-bar"><div class="gauge-fill" style="width:${c.econ_pct}%;background:${econColor}"></div></div></div></td><td class="${netClass}">${c.cot_net.toLocaleString()}</td><td class="${changeClass}">${c.cot_change.toLocaleString()}</td><td>${c.longs.toLocaleString()}</td><td>${c.shorts.toLocaleString()}</td></tr>`});html+='</tbody></table>';document.getElementById('currencyTable').innerHTML=html}
function renderBarChart(currencies){const ctx=document.getElementById('cotBarChart').getContext('2d');const labels=currencies.map(c=>c.currency);const longPcts=currencies.map(c=>c.long_pct);const shortPcts=currencies.map(c=>c.short_pct);if(window.barChart) window.barChart.destroy();window.barChart=new Chart(ctx,{type:'bar',data:{labels:labels,datasets:[{label:'Long %',data:longPcts,backgroundColor:'rgba(0,229,160,0.7)',borderColor:'#00e5a0',borderWidth:1},{label:'Short %',data:shortPcts,backgroundColor:'rgba(255,77,109,0.7)',borderColor:'#ff4d6d',borderWidth:1}]},options:{responsive:true,maintainAspectRatio:true,scales:{x:{title:{display:true,text:'Currency',color:'#a0b0c0'},ticks:{color:'#fff'}},y:{title:{display:true,text:'Percentage (%)',color:'#a0b0c0'},ticks:{color:'#fff',beginAtZero:true,max:100}}},plugins:{legend:{labels:{color:'#fff'},position:'top'},tooltip:{callbacks:{label:function(context){return `${context.dataset.label}: ${context.raw.toFixed(1)}%`}}}}}})}
function logout(){fetch('/logout').then(()=>window.location.href='/login')}
loadCurrencies();
</script>
</body>
</html>''')

    # Sentiment
    with open('templates/sentiment.html', 'w') as f:
        f.write('''<!DOCTYPE html>
<html><head><title>Sentiment – Tradion</title><meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI',sans-serif;background:#0B0F1A;color:#fff;display:flex}.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;left:0;top:0;border-right:1px solid #2a3040;z-index:100}.sidebar .logo{font-size:22px;color:#00e5ff;text-align:center;margin-bottom:30px}.sidebar .menu-item{padding:12px 15px;margin:5px 0;border-radius:8px;cursor:pointer;color:#a0b0c0}.sidebar .menu-item:hover{background:rgba(0,229,255,0.1);color:#00e5ff}.sidebar .menu-item.active{background:linear-gradient(135deg,#00e5ff,#00b8d4);color:#0B0F1A}.main-content{flex:1;margin-left:280px;padding:20px 30px}.header h2{color:#00e5ff;margin-bottom:20px}.search-box{padding:10px 15px;background:#1a1f2e;border:1px solid #2a3040;border-radius:8px;color:#fff;width:100%;max-width:350px;margin-bottom:20px}.sentiment-table{width:100%;border-collapse:collapse;background:#121826;border-radius:12px;overflow:hidden}.sentiment-table th,.sentiment-table td{padding:12px 15px;border-bottom:1px solid #2a3040;text-align:left}.sentiment-table th{background:rgba(0,229,255,0.1);color:#00e5ff}.sentiment-table td{padding:12px 15px}.bar-container{display:flex;align-items:center;gap:10px}.bar-wrapper{flex:1;height:24px;background:#1a1f2e;border-radius:12px;overflow:hidden;display:flex}.bar-long{background:#00e5a0;height:100%;display:flex;align-items:center;justify-content:center;color:#0B0F1A;font-weight:bold;font-size:0.8rem;transition:width 0.3s}.bar-short{background:#ff4d6d;height:100%;display:flex;align-items:center;justify-content:center;color:#fff;font-weight:bold;font-size:0.8rem;transition:width 0.3s}.percentage-col{width:80px;text-align:right}.pair-col{font-weight:bold;width:120px}
</style>
</head>
<body>
<div class="sidebar"><div class="logo">📊 Tradion</div><div class="menu-item" onclick="window.location.href='/dashboard'">Dashboard</div><div class="menu-item" onclick="window.location.href='/currencies'">COT Data</div><div class="menu-item" onclick="window.location.href='/scorecard'">Scorecard</div><div class="menu-item active" onclick="window.location.href='/sentiment'">Sentiment</div><div class="menu-item" onclick="window.location.href='/profile'">Profile</div><div class="menu-item" onclick="logout()">Logout</div></div>
<div class="main-content">
    <div class="header"><h2>Market Sentiment</h2></div>
    <input type="text" id="searchSentiment" class="search-box" placeholder="Search pair...">
    <table class="sentiment-table" id="sentimentTable"><thead><tr><th>Pair</th><th>Sentiment Bar</th><th>Long %</th><th>Short %</th></tr></thead><tbody></tbody></table>
</div>
<script>
async function loadSentiment(){const res=await fetch('/api/sentiment');const data=await res.json();renderSentiment(data)}
function renderSentiment(data){const tbody=document.querySelector('#sentimentTable tbody');tbody.innerHTML='';data.forEach(s=>{const long=s.long_pct,short=s.short_pct;const row=document.createElement('tr');row.innerHTML=`<td class="pair-col">${s.pair}</td><td><div class="bar-wrapper"><div class="bar-long" style="width:${long}%">${long>0?long.toFixed(0)+'%':''}</div><div class="bar-short" style="width:${short}%">${short>0?short.toFixed(0)+'%':''}</div></div></td><td class="percentage-col" style="color:#00e5a0">${long}%</td><td class="percentage-col" style="color:#ff4d6d">${short}%</td>`;tbody.appendChild(row)})}
document.getElementById('searchSentiment').addEventListener('input',function(){const term=this.value.toLowerCase();const rows=document.querySelectorAll('#sentimentTable tbody tr');rows.forEach(row=>{const pair=row.querySelector('.pair-col').textContent.toLowerCase();row.style.display=pair.includes(term)?'':'none'})});
function logout(){fetch('/logout').then(()=>window.location.href='/login')}
loadSentiment();
</script>
</body>
</html>''')

    # Profile
    with open('templates/profile.html', 'w') as f:
        f.write('''<!DOCTYPE html>
<html><head><title>Profile - Tradion</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI',sans-serif;background:#0B0F1A;color:#fff;display:flex}.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed}.logo{font-size:24px;color:#00e5ff;margin-bottom:30px}.menu-item{padding:12px 15px;margin:5px 0;border-radius:8px;cursor:pointer;color:#a0b0c0}.menu-item:hover{background:rgba(0,229,255,0.1);color:#00e5ff}.main-content{flex:1;margin-left:280px;padding:30px}.container{max-width:600px;margin:50px auto;padding:30px;background:#121826;border-radius:20px;border:1px solid #2a3040}.container h2{color:#00e5ff;margin-bottom:20px}.container div{margin-bottom:20px;font-size:1.1em}.btn{display:inline-block;padding:10px 20px;background:#00e5ff;color:#000;text-decoration:none;border-radius:8px;font-weight:bold;margin-top:10px}</style>
</head>
<body>
<div class="sidebar"><div class="logo">⚡ Tradion</div><div class="menu-item" onclick="window.location.href='/dashboard'">📊 Dashboard</div><div class="menu-item" onclick="logout()">🚪 Logout</div></div>
<div class="main-content"><div class="container"><h2>Profile</h2><div>👤 Username: <strong>{{ username }}</strong></div><a href="/dashboard" class="btn">← Back to Dashboard</a></div></div>
<script>function logout(){fetch('/logout').then(()=>window.location.href='/login');}</script>
</body>
</html>''')

    # Admin (complete, no truncation)
    with open('templates/admin.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html><head><title>Admin - Tradion</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI',sans-serif;background:#0B0F1A;color:#fff}.sidebar{width:280px;background:#121826;min-height:100vh;padding:20px;position:fixed;z-index:100}.logo{font-size:22px;color:#00e5ff;margin-bottom:30px}.menu-item{padding:12px 15px;margin:5px 0;border-radius:8px;cursor:pointer;color:#a0b0c0}.menu-item:hover{background:rgba(0,229,255,0.1);color:#00e5ff}.main-content{margin-left:280px;padding:20px}.card{background:#121826;border-radius:12px;padding:20px;margin-bottom:20px;border:1px solid #2a3040}button{padding:8px 16px;background:#00e5ff;color:#0B0F1A;border:none;border-radius:6px;cursor:pointer;font-weight:bold}button.secondary{background:transparent;border:1px solid #00e5ff;color:#00e5ff}.content-pane{display:none}.content-pane.active{display:block}table{width:100%;border-collapse:collapse}th,td{padding:8px;border-bottom:1px solid #2a3040;text-align:left}input,select{padding:6px;background:#1a1f2e;border:1px solid #2a3040;color:#fff;border-radius:4px;width:100%;margin-bottom:10px}.modal{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.7);z-index:1000;justify-content:center;align-items:center}.modal-content{background:#121826;padding:25px;border-radius:16px;width:90%;max-width:500px;border:1px solid #2a3040}.month-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin:15px 0}

/* COT Data Table improvements */
.cot-table-container {
    overflow-x: auto;
    margin-top: 20px;
    border-radius: 12px;
    border: 1px solid #2a3040;
}
.cot-data-table {
    width: 100%;
    border-collapse: collapse;
    background: #121826;
    font-size: 14px;
}
.cot-data-table th,
.cot-data-table td {
    padding: 14px 12px;
    text-align: left;
    border-bottom: 1px solid #2a3040;
    vertical-align: middle;
}
.cot-data-table th {
    background: rgba(0, 229, 255, 0.15);
    color: #00e5ff;
    font-weight: 600;
    font-size: 0.9rem;
}
.cot-data-table td {
    color: #e0e0e0;
}
.cot-data-table tr:hover {
    background: rgba(0, 229, 255, 0.05);
}
.inline-edit {
    background: #1a1f2e;
    border: 1px solid #3a4055;
    padding: 8px 10px;
    border-radius: 6px;
    color: #fff;
    width: 120px;
    font-size: 0.9rem;
    transition: all 0.2s;
}
.inline-edit:focus {
    outline: none;
    border-color: #00e5ff;
    box-shadow: 0 0 0 2px rgba(0,229,255,0.2);
}
.save-cot-btn {
    background: #00e5ff;
    color: #0B0F1A;
    border: none;
    padding: 6px 12px;
    border-radius: 6px;
    cursor: pointer;
    font-weight: bold;
    font-size: 0.8rem;
    transition: 0.2s;
}
.save-cot-btn:hover {
    background: #00b8d4;
    transform: scale(1.02);
}
.cot-refresh-btn, .cot-add-btn {
    background: transparent;
    border: 1px solid #00e5ff;
    color: #00e5ff;
    padding: 6px 12px;
    border-radius: 6px;
    cursor: pointer;
    font-weight: bold;
    margin-left: 8px;
}
.cot-refresh-btn:hover, .cot-add-btn:hover {
    background: rgba(0,229,255,0.1);
}
.cot-title-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
    flex-wrap: wrap;
    gap: 15px;
}
.cot-title-bar h2 {
    color: #00e5ff;
    font-size: 1.5rem;
    margin: 0;
}
</style>
</head>
<body>
<div class="sidebar">
    <div class="logo">👑 ADMIN PANEL</div>
    <div class="menu-item" onclick="showPane('cotUpload')">📁 COT Upload</div>
    <div class="menu-item" onclick="showPane('econUpload')">📊 Econ Upload</div>
    <div class="menu-item" onclick="showPane('econIndicators')">📉 Indicators</div>
    <div class="menu-item" onclick="showPane('cotData')">📈 COT Data</div>
    <div class="menu-item" onclick="showPane('seasonality')">📅 Seasonality</div>
    <div class="menu-item" onclick="showPane('sentiment')">💬 Sentiment</div>
    <div class="menu-item" onclick="showPane('users')">👥 Users</div>
    <div class="menu-item" onclick="window.location.href='/dashboard'">⬅ Dashboard</div>
</div>
<div class="main-content">
    <div id="cotUploadPane" class="content-pane active"><div class="card"><h2>Upload COT Data</h2><input type="file" id="cotFile" accept=".xlsx"><button onclick="uploadCOT()">Upload</button><div id="cotStatus" style="margin-top:10px"></div></div></div>
    <div id="econUploadPane" class="content-pane"><div class="card"><h2>Upload Economic Data</h2><input type="file" id="econFile" accept=".xlsx"><button onclick="uploadEcon()">Upload</button><div id="econStatus" style="margin-top:10px"></div></div></div>
    <div id="econIndicatorsPane" class="content-pane"><div class="card"><h2>Economic Indicators</h2><label>Currency: <select id="econCurrencySelect" onchange="loadIndicators()"><option value="">-- choose --</option></select></label><div style="margin:15px 0"><button onclick="openIndicatorModal()">+ Add Indicator</button></div><div id="indicatorsList"></div></div></div>
    
    <!-- IMPROVED COT DATA PANEL with Add Currency button -->
    <div id="cotDataPane" class="content-pane">
        <div class="card">
            <div class="cot-title-bar">
                <h2>📊 COT Data · Non‑Commercial Positions</h2>
                <div>
                    <button onclick="addNewCurrency()" class="cot-add-btn">➕ Add Currency</button>
                    <button onclick="loadCOTData()" class="cot-refresh-btn">🔄 Refresh</button>
                </div>
            </div>
            <div class="cot-table-container">
                <table class="cot-data-table" id="cotDataTable">
                    <thead>
                        <tr><th>Currency</th><th>Longs (contracts)</th><th>Shorts (contracts)</th><th>Net Position</th><th>Weekly Change</th><th>Last Updated</th><th>Actions</th></tr>
                    </thead>
                    <tbody id="cotDataList"><tr><td colspan="7" style="text-align:center">Loading...</td></tr></tbody>
                </table>
            </div>
        </div>
    </div>
    
    <div id="seasonalityPane" class="content-pane"><div class="card"><h2>Seasonality Configuration</h2><label>Pair: <select id="seasonPairSelect" onchange="loadSeasonConfig()"><option value="">-- select pair --</option></select></label><h3 style="margin:20px 0 10px">Monthly Bias</h3><div id="monthlyBiasContainer"></div><button onclick="saveMonthlyBiases()" style="margin:10px 0">Save Monthly Biases</button><h3 style="margin:20px 0 10px">Date Ranges</h3><button onclick="addDateRange()">+ Add Date Range</button><div id="dateRangeList" style="margin-top:10px"></div></div></div>
    <div id="sentimentPane" class="content-pane"><div class="card"><h2>Sentiment Data</h2><div style="margin-bottom:15px"><button onclick="openSentimentModal()">+ Add/Update Sentiment</button></div><div id="sentimentList"></div></div></div>
    <div id="usersPane" class="content-pane"><div class="card"><h2>User Management</h2><button onclick="loadUsers()">Refresh</button><div id="usersList" style="margin-top:15px"></div></div></div>
</div>

<!-- Indicator Modal -->
<div class="modal" id="indicatorModal"><div class="modal-content"><h3 id="indModalTitle" style="color:#00e5ff;margin-bottom:20px">Add Indicator</h3><form id="indicatorForm"><input type="hidden" id="indicatorId"><label>Currency</label><select id="indCurrencySelect" required></select><label>Indicator Name</label><input type="text" id="indName" required><div style="display:grid;grid-template-columns:1fr 1fr;gap:10px"><div><label>Forecast</label><input type="number" step="0.1" id="indForecast" required></div><div><label>Actual</label><input type="number" step="0.1" id="indActual" required></div></div><label><input type="checkbox" id="indLowerBetter"> Lower is better</label><label>Category</label><select id="indCategory" required><option value="Technical Bias">Technical Bias</option><option value="Economic Growth Bias">Economic Growth Bias</option><option value="Inflation Bias">Inflation Bias</option><option value="Jobs Market Bias">Jobs Market Bias</option><option value="Crowd Sentiment (COT)">Crowd Sentiment (COT)</option></select><div style="display:flex;gap:10px;margin-top:20px"><button type="submit">Save</button><button type="button" onclick="closeIndicatorModal()" class="secondary">Cancel</button></div></form></div></div>

<!-- Sentiment Modal -->
<div class="modal" id="sentimentModal"><div class="modal-content"><h3 style="color:#00e5ff;margin-bottom:20px">Sentiment Entry</h3><form id="sentimentForm"><label>Pair</label><select id="sentPair" required></select><label>Long %</label><input type="number" step="0.1" id="sentLong" min="0" max="100" required><label>Short %</label><input type="number" step="0.1" id="sentShort" min="0" max="100" required><div style="display:flex;gap:10px;margin-top:20px"><button type="submit">Save</button><button type="button" onclick="closeSentimentModal()" class="secondary">Cancel</button></div></form></div></div>

<script>
const allPairs={{ ALL_PAIRS|tojson }};
const currencies=["USD","EUR","GBP","JPY","AUD","CAD","CHF","NZD","XAU","BTC"];
let seasonCurrentPair='';

function showPane(pane){
    document.querySelectorAll('.content-pane').forEach(p=>p.classList.remove('active'));
    document.getElementById(pane+'Pane').classList.add('active');
    if(pane==='users') loadUsers();
    if(pane==='seasonality') populateSeasonPairs();
    if(pane==='econIndicators') populateCurrencySelects();
    if(pane==='cotData') loadCOTData();
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

// --- ADD NEW CURRENCY ---
async function addNewCurrency(){
    let currency = prompt("Enter currency code (e.g., EUR, GBP, JPY, AUD, CAD, CHF, NZD):");
    if(!currency) return;
    currency = currency.toUpperCase().trim();
    if(!currencies.includes(currency)){
        alert("Invalid currency. Use one of: " + currencies.join(", "));
        return;
    }
    try{
        const res = await fetch('/admin/cot_data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ currency, longs: 0, shorts: 0, weekly_change: 0 })
        });
        const result = await res.json();
        if(result.success){
            alert(`✅ ${currency} added with default zeros. Edit values and click Save.`);
            loadCOTData();  // refresh table
        } else {
            alert("❌ Failed to add currency. It may already exist.");
        }
    } catch(e){
        alert("Error: " + e.message);
    }
}

async function loadIndicators(){
    const cur=document.getElementById('econCurrencySelect').value;
    if(!cur){document.getElementById('indicatorsList').innerHTML='';return;}
    const res=await fetch('/admin/econ_indicators?currency='+cur);
    const data=await res.json();
    let html='<table><thead><tr><th>Indicator</th><th>Forecast</th><th>Actual</th><th>Category</th><th>Lower</th><th>Actions</th></tr></thead><tbody>';
    data.forEach(ind=>{html+=`<tr><td>${ind.indicator_name}</td><td>${ind.forecast}</td><td>${ind.actual}</td><td>${ind.category||'General'}</td><td>${ind.is_lower_better?'Yes':'No'}</td><td><button onclick="editIndicator(${ind.id})">Edit</button> <button onclick="deleteIndicator(${ind.id})">Del</button></td></tr>`});
    html+='</tbody></table>'; document.getElementById('indicatorsList').innerHTML=html;
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

// --- COT Data inline editing ---
async function loadCOTData(){
    const tbody=document.getElementById('cotDataList');
    tbody.innerHTML='<tr><td colspan="7" style="text-align:center">Loading...</td></tr>';
    try{
        const res=await fetch('/admin/cot_data');
        const data=await res.json();
        if(!data.length){tbody.innerHTML='<tr><td colspan="7" style="text-align:center">No data. Click "Add Currency" to create entries.</td></tr>';return;}
        let html='';
        data.forEach(cot=>{
            html+=`<tr data-id="${cot.id}" data-currency="${cot.currency}">
                <td><strong>${cot.currency}</strong></td>
                <td><input type="number" step="1" id="longs_${cot.currency}" value="${cot.longs}" class="inline-edit" style="width:120px"></td>
                <td><input type="number" step="1" id="shorts_${cot.currency}" value="${cot.shorts}" class="inline-edit" style="width:120px"></td>
                <td id="net_${cot.currency}">${cot.net_position}</td>
                <td><input type="number" step="1" id="change_${cot.currency}" value="${cot.weekly_change}" class="inline-edit" style="width:100px"></td>
                <td>${cot.last_updated?new Date(cot.last_updated).toLocaleString():'Never'}</td>
                <td><button class="save-cot-btn" onclick="saveCOTRow('${cot.currency}')">💾 Save</button></td>
            </tr>`;
        });
        tbody.innerHTML=html;
    } catch(e){ tbody.innerHTML='<tr><td colspan="7" style="color:#ff4d6d">Error loading data</td></tr>'; console.error(e); }
}
async function saveCOTRow(currency){
    const longs=parseFloat(document.getElementById(`longs_${currency}`).value);
    const shorts=parseFloat(document.getElementById(`shorts_${currency}`).value);
    const weekly_change=parseFloat(document.getElementById(`change_${currency}`).value);
    if(isNaN(longs)||isNaN(shorts)||isNaN(weekly_change)){ alert("Please enter valid numbers"); return; }
    try{
        const res=await fetch('/admin/cot_data',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({currency,longs,shorts,weekly_change})});
        const result=await res.json();
        if(result.success){
            document.getElementById(`net_${currency}`).innerText=longs-shorts;
            alert(`✅ ${currency} COT data saved`);
        } else alert("❌ Save failed");
    } catch(e){ alert("Error: "+e.message); }
}

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
    let html='<table><thead><tr><th>User</th><th>Email</th><th>Admin</th><th>Status</th><th>Actions</th></tr></thead><tbody>';
    data.users.forEach(u=>{
        const status=u.is_active?'Active':'Inactive';
        const badgeColor=u.is_active?'#00e5a0':'#ff4d6d';
        const actionBtn=u.is_active?`<button onclick="deactivateUser(${u.id})" class="secondary">Deactivate</button>`:`<button onclick="activateUser(${u.id})">Activate</button>`;
        html+=`<tr><td>${u.username}</td><td>${u.email}</td><td>${u.is_admin?'Yes':'No'}</td><td style="color:${badgeColor}">${status}</td><td>${actionBtn}</td></tr>`;
    });
    html+='</tbody></table>'; document.getElementById('usersList').innerHTML=html;
}
async function activateUser(id){await fetch(`/admin/users/${id}/activate`,{method:'POST'});loadUsers();}
async function deactivateUser(id){await fetch(`/admin/users/${id}/deactivate`,{method:'POST'});loadUsers();}

// Seasonality
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
    let html='<table><thead><tr><th>Start MM/DD</th><th>End MM/DD</th><th>Bias</th><th>Actions</th></tr></thead><tbody>';
    ranges.forEach(r=>{html+=`<tr><td>${r.start_month}/${r.start_day}</td><td>${r.end_month}/${r.end_day}</td><td>${r.bias}</td><td><button onclick="deleteDateRange(${r.id})">Delete</button></td></tr>`});
    html+='</tbody></table>'; document.getElementById('dateRangeList').innerHTML=html;
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

// Sentiment
async function loadSentiment(){
    const res=await fetch('/admin/sentiment');
    const data=await res.json();
    let html='<table><thead><tr><th>Pair</th><th>Long %</th><th>Short %</th><th>Actions</th></tr></thead><tbody>';
    data.forEach(s=>{html+=`<tr><td>${s.pair}</td><td>${s.long_pct}%</td><td>${s.short_pct}%</td><td><button onclick="editSentiment('${s.pair}',${s.long_pct},${s.short_pct})">Edit</button> <button onclick="deleteSentiment(${s.id})">Del</button></td></tr>`});
    html+='</tbody></table>'; document.getElementById('sentimentList').innerHTML=html;
}
function openSentimentModal(){
    document.getElementById('sentimentForm').reset();
    document.getElementById('sentimentModal').style.display='flex';
    const select=document.getElementById('sentPair');
    select.innerHTML='';
    allPairs.forEach(p=>{const pairStr=p[0]+'/'+p[1];const opt=document.createElement('option');opt.value=pairStr;opt.textContent=pairStr;select.appendChild(opt)});
}
function closeSentimentModal(){document.getElementById('sentimentModal').style.display='none';}
function editSentiment(pair,long,short){ openSentimentModal(); document.getElementById('sentPair').value=pair; document.getElementById('sentLong').value=long; document.getElementById('sentShort').value=short; }
async function deleteSentiment(id){ if(confirm('Delete?')){await fetch('/admin/sentiment/'+id,{method:'DELETE'});loadSentiment();} }
document.getElementById('sentimentForm').addEventListener('submit',async e=>{
    e.preventDefault();
    const data={pair:document.getElementById('sentPair').value,long_pct:parseFloat(document.getElementById('sentLong').value),short_pct:parseFloat(document.getElementById('sentShort').value)};
    await fetch('/admin/sentiment',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    closeSentimentModal(); loadSentiment();
});

populateCurrencySelects();
loadUsers();
</script>
</body>
</html>''')

# -----------------------------
# MAIN
# -----------------------------
if __name__ == '__main__':
    #migrate_database()
    create_templates()
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
    print("=" * 50)
    app.run(debug=False, use_reloader=False, host='0.0.0.0', port=5000)