"""
SHIVA ML Engine — Online learning filter for OB_SMC trades.
Trains on closed trade history, predicts WIN probability per signal.
Blocks low-confidence entries (< threshold) to improve effective WR.

Learning loop:
  1. OB_SMC fires → extract 15 features from bar state
  2. ML predicts WIN probability → block if < CONFIDENCE_THRESHOLD
  3. Trade closes → label WIN/LOSS → add to buffer
  4. Every RETRAIN_EVERY new trades → retrain RandomForest
  5. Model persists to disk across restarts
"""

import os
import pickle
import numpy as np
import pandas as pd
from pathlib import Path

try:
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import cross_val_score
    from sklearn.metrics import classification_report
    _SKLEARN = True
except ImportError:
    _SKLEARN = False

MODEL_PATH = Path(os.environ.get('SHIVA_MODEL_PATH', '/tmp/shiva_ml.pkl'))

FEATURES = [
    'adx', 'rsi', 'atr_pct', 'ema200_dist_pct',
    'ob_age', 'ob_size_atr', 'hour', 'dow',
    'momentum_5', 'momentum_20', 'vol_ratio',
    'direction', 'bb_pct', 'trend_strength', 'rsi_slope'
]

MIN_TRADES         = 20    # trades before ML activates
RETRAIN_EVERY      = 5     # retrain after N new trades
CONFIDENCE_THRESH  = 0.58  # min predicted WIN probability to trade
MAX_HISTORY        = 300   # rolling window size


class MLTradeFilter:
    """
    Online-learning ML gate. Drop-in filter after any strategy signal.
    Usage:
        ml = MLTradeFilter()
        features = ml.extract_features(df, direction=1)
        conf, ok  = ml.predict(features)
        # ... trade executes ...
        ml.add_result(features, label=1)  # 1=WIN 0=LOSS
    """

    def __init__(self, confidence_threshold: float = CONFIDENCE_THRESH):
        self.threshold   = confidence_threshold
        self.model       = None
        self.scaler      = StandardScaler() if _SKLEARN else None
        self.buffer: list[tuple[dict, int]] = []
        self.new_since_retrain = 0
        self.trained     = False
        self.cv_accuracy = 0.0
        self._load()

    # ── Feature extraction ────────────────────────────────────────────────
    def extract_features(self, df: pd.DataFrame, direction: int,
                         ob_age: int = 0, ob_size_atr: float = 0.0) -> dict:
        r     = df.iloc[-1]
        close = float(r.get('close', 0) or 0)
        if close == 0:
            return self._zero_features(direction)

        ema200   = float(r.get('EMA_200',  close) or close)
        atr      = float(r.get('ATR_14',   1.0)   or 1.0)
        adx      = float(r.get('ADX_14',   25.0)  or 25.0)
        rsi      = float(r.get('RSI_14',   50.0)  or 50.0)
        bb_upper = float(r.get('BBU_20_2.0', close + atr * 2) or close + atr * 2)
        bb_lower = float(r.get('BBL_20_2.0', close - atr * 2) or close - atr * 2)

        atr_pct          = atr / close * 100
        ema200_dist_pct  = (close - ema200) / ema200 * 100 if ema200 > 0 else 0.0
        bb_range         = bb_upper - bb_lower
        bb_pct           = (close - bb_lower) / bb_range if bb_range > 0 else 0.5

        # 5-bar and 20-bar momentum
        momentum_5  = 0.0
        momentum_20 = 0.0
        if len(df) >= 5:
            p5 = float(df.iloc[-5]['close'] or close)
            momentum_5 = (close - p5) / p5 * 100
        if len(df) >= 20:
            p20 = float(df.iloc[-20]['close'] or close)
            momentum_20 = (close - p20) / p20 * 100

        # Volume ratio (current vs 20-bar avg)
        vol     = float(r.get('volume', 1.0) or 1.0)
        vol_avg = float(df['volume'].tail(20).mean() or 1.0)
        vol_ratio = min(vol / vol_avg if vol_avg > 0 else 1.0, 5.0)

        # RSI slope (current vs 5 bars ago)
        rsi_slope = 0.0
        if len(df) >= 5 and 'RSI_14' in df.columns:
            rsi_5 = float(df.iloc[-5].get('RSI_14', rsi) or rsi)
            rsi_slope = rsi - rsi_5

        # Time features
        hour, dow = 10, 1
        try:
            import pytz
            t = df.index[-1]
            t_est = t.tz_convert(pytz.timezone('US/Eastern'))
            hour, dow = t_est.hour, t_est.weekday()
        except Exception:
            pass

        trend_strength = int(adx > 20) + int(adx > 30) + int(adx > 40)

        return {
            'adx':             adx,
            'rsi':             rsi,
            'atr_pct':         atr_pct,
            'ema200_dist_pct': ema200_dist_pct,
            'ob_age':          float(ob_age),
            'ob_size_atr':     float(ob_size_atr),
            'hour':            float(hour),
            'dow':             float(dow),
            'momentum_5':      momentum_5,
            'momentum_20':     momentum_20,
            'vol_ratio':       vol_ratio,
            'direction':       float(direction),
            'bb_pct':          bb_pct,
            'trend_strength':  float(trend_strength),
            'rsi_slope':       rsi_slope,
        }

    def _zero_features(self, direction: int) -> dict:
        return {f: 0.0 for f in FEATURES} | {'direction': float(direction)}

    # ── Prediction ────────────────────────────────────────────────────────
    def predict(self, features: dict) -> tuple[float, bool]:
        """
        Returns (win_probability, should_enter).
        Before MIN_TRADES or sklearn missing → always allow.
        """
        if not self.trained or not _SKLEARN:
            return 0.5, True
        try:
            X = np.array([[features.get(f, 0.0) for f in FEATURES]])
            X_s = self.scaler.transform(X)
            prob = float(self.model.predict_proba(X_s)[0][1])
            return prob, prob >= self.threshold
        except Exception:
            return 0.5, True

    # ── Learning ──────────────────────────────────────────────────────────
    def add_result(self, features: dict, label: int):
        """Call after each trade closes. label: 1=WIN, 0=LOSS."""
        self.buffer.append((features, label))
        if len(self.buffer) > MAX_HISTORY:
            self.buffer = self.buffer[-MAX_HISTORY:]
        self.new_since_retrain += 1
        if (len(self.buffer) >= MIN_TRADES
                and self.new_since_retrain >= RETRAIN_EVERY):
            self._train()

    def _train(self):
        if not _SKLEARN or len(self.buffer) < MIN_TRADES:
            return
        X = np.array([[t[0].get(f, 0.0) for f in FEATURES] for t in self.buffer])
        y = np.array([t[1] for t in self.buffer], dtype=int)
        if len(set(y)) < 2:
            return
        try:
            self.scaler.fit(X)
            Xs = self.scaler.transform(X)
            n_cv = min(3, max(2, len(y) // 10))
            rf   = RandomForestClassifier(
                n_estimators=200, max_depth=5, min_samples_leaf=3,
                class_weight='balanced', random_state=42
            )
            self.model   = CalibratedClassifierCV(rf, cv=n_cv, method='isotonic')
            self.model.fit(Xs, y)
            self.trained = True
            self.new_since_retrain = 0

            # CV accuracy for logging
            try:
                rf2    = RandomForestClassifier(n_estimators=100, max_depth=5,
                                                random_state=42)
                scores = cross_val_score(rf2, Xs, y, cv=n_cv, scoring='accuracy')
                self.cv_accuracy = float(scores.mean())
            except Exception:
                self.cv_accuracy = 0.0

            wins = int(y.sum())
            print(f"🧠 ML retrained | trades={len(y)} wins={wins} "
                  f"WR={wins/len(y)*100:.0f}% cv_acc={self.cv_accuracy:.1%} "
                  f"threshold={self.threshold:.0%}")
            self._save()
        except Exception as e:
            print(f"⚠️  ML train error: {e}")

    # ── Feature importance report ─────────────────────────────────────────
    def feature_importance(self) -> pd.DataFrame:
        if not self.trained or not _SKLEARN:
            return pd.DataFrame()
        try:
            base = self.model.estimator
            imp  = base.feature_importances_
            return (pd.DataFrame({'feature': FEATURES, 'importance': imp})
                    .sort_values('importance', ascending=False)
                    .reset_index(drop=True))
        except Exception:
            return pd.DataFrame()

    # ── Stats ─────────────────────────────────────────────────────────────
    @property
    def stats(self) -> dict:
        if not self.buffer:
            return {'total': 0, 'trained': False}
        y = [t[1] for t in self.buffer]
        return {
            'total':    len(y),
            'wins':     sum(y),
            'wr':       round(sum(y) / len(y) * 100, 1),
            'trained':  self.trained,
            'cv_acc':   round(self.cv_accuracy * 100, 1),
            'threshold': self.threshold,
        }

    # ── Persistence ───────────────────────────────────────────────────────
    def _save(self):
        try:
            with open(MODEL_PATH, 'wb') as f:
                pickle.dump({
                    'model': self.model, 'scaler': self.scaler,
                    'buffer': self.buffer, 'cv_accuracy': self.cv_accuracy
                }, f)
        except Exception:
            pass

    def _load(self):
        try:
            if MODEL_PATH.exists():
                with open(MODEL_PATH, 'rb') as f:
                    d = pickle.load(f)
                self.model       = d['model']
                self.scaler      = d['scaler']
                self.buffer      = d.get('buffer', [])
                self.cv_accuracy = d.get('cv_accuracy', 0.0)
                self.trained     = True
                s = self.stats
                print(f"🧠 ML model loaded | trades={s['total']} "
                      f"WR={s['wr']}% cv_acc={s['cv_acc']}%")
        except Exception:
            pass
