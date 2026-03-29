"""Tests for signal detectors — run with: pytest tests/"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from app.detectors import bulk_deal, insider_trade, confluence
from app.config import SignalType, Severity


def test_bulk_deal_buy_signal():
    deals = [
        {
            "date": "2026-03-27", "symbol": "RELIANCE",
            "client_name": "HDFC Mutual Fund",
            "deal_type": "BUY", "quantity": 1_000_000,
            "price": 2840.0, "value_cr": 284.0, "exchange": "NSE",
        }
    ]
    signals = bulk_deal.detect(deals)
    assert len(signals) == 1
    assert signals[0]["symbol"] == "RELIANCE"
    assert signals[0]["signal_type"] == SignalType.BULK_DEAL_BUY
    assert signals[0]["severity"] == Severity.HIGH


def test_bulk_deal_below_threshold_ignored():
    deals = [
        {
            "date": "2026-03-27", "symbol": "SMALLCAP",
            "client_name": "Retail Investor",
            "deal_type": "BUY", "quantity": 100,
            "price": 50.0, "value_cr": 0.0005, "exchange": "NSE",
        }
    ]
    signals = bulk_deal.detect(deals)
    assert len(signals) == 0


def test_insider_promoter_buy():
    trades = [
        {
            "date": "2026-03-27", "symbol": "ITC",
            "person_name": "Sanjiv Puri", "person_category": "Promoter",
            "trade_type": "BUY", "quantity": 50_000, "price": 476.0,
            "value_lakh": 238.0, "holding_pre": 43.8, "holding_post": 43.85,
            "exchange": "NSE",
        }
    ]
    signals = insider_trade.detect(trades)
    assert len(signals) == 1
    assert signals[0]["signal_type"] == SignalType.INSIDER_BUY
    assert signals[0]["raw_data"]["is_promoter_involved"] is True


def test_confluence_detected():
    signals = [
        {"symbol": "TATAMOTORS", "signal_type": SignalType.INSIDER_BUY, "severity": Severity.HIGH, "metric_value": 100},
        {"symbol": "TATAMOTORS", "signal_type": SignalType.VOLUME_SPIKE, "severity": Severity.MEDIUM, "metric_value": 3.2},
        {"symbol": "TATAMOTORS", "signal_type": SignalType.BULK_DEAL_BUY, "severity": Severity.HIGH, "metric_value": 200},
    ]
    conf = confluence.detect(signals)
    assert len(conf) == 1
    assert conf[0]["signal_type"] == SignalType.CONFLUENCE
    assert conf[0]["severity"] == Severity.HIGH
    assert conf[0]["raw_data"]["direction"] == "BULLISH"


def test_mixed_signals_no_confluence():
    signals = [
        {"symbol": "WIPRO", "signal_type": SignalType.INSIDER_BUY, "severity": Severity.MEDIUM, "metric_value": 50},
        {"symbol": "WIPRO", "signal_type": SignalType.BULK_DEAL_SELL, "severity": Severity.HIGH, "metric_value": 300},
    ]
    conf = confluence.detect(signals)
    # Mixed signals — no confluence
    assert len(conf) == 0
