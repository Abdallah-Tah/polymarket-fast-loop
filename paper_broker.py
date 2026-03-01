"""Paper broker for Polymarket FastLoop.

This is a lightweight paper-trading engine:
- Uses real Polymarket CLOB orderbooks/midpoints for pricing
- Maintains a persistent ledger (cash + positions)
- Simulates realistic execution (bid/ask + bps slippage + latency)

No wallet keys. No real orders.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class PaperPosition:
    token_id: str
    side: str  # "yes" or "no"
    shares: float
    avg_entry: float
    market_slug: str
    question: str = ""
    updated_at: float = 0.0


class PaperBroker:
    def __init__(
        self,
        ledger_path: Path,
        trades_path: Path,
        start_usd: float = 100.0,
        slippage_bps: float = 25.0,
        latency_ms: int = 350,
    ):
        self.ledger_path = ledger_path
        self.trades_path = trades_path
        self.start_usd = float(start_usd)
        self.slippage_bps = float(slippage_bps)
        self.latency_ms = int(latency_ms)
        self._state: Dict[str, Any] = self._load_or_init()

    def _load_or_init(self) -> Dict[str, Any]:
        if self.ledger_path.exists():
            try:
                return json.loads(self.ledger_path.read_text())
            except Exception:
                pass
        state = {
            "version": 1,
            "created_at": time.time(),
            "cash_usd": self.start_usd,
            "realized_pnl_usd": 0.0,
            "positions": {},  # key -> PaperPosition dict
        }
        self._save(state)
        return state

    def _save(self, state: Optional[Dict[str, Any]] = None) -> None:
        if state is not None:
            self._state = state
        tmp = self.ledger_path.with_suffix(self.ledger_path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._state, indent=2, sort_keys=True) + "\n")
        tmp.replace(self.ledger_path)

    def snapshot(self) -> Dict[str, Any]:
        return self._state

    def _trade_log(self, row: Dict[str, Any]) -> None:
        self.trades_path.parent.mkdir(parents=True, exist_ok=True)
        with self.trades_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, sort_keys=True) + "\n")

    def _apply_slippage(self, price: float, is_buy: bool = True) -> float:
        # For buys: worse price (higher). For sells (not used yet): lower.
        mult = 1.0 + (self.slippage_bps / 10000.0) if is_buy else 1.0 - (self.slippage_bps / 10000.0)
        return max(0.0001, min(0.9999, price * mult))

    def place_market_buy(
        self,
        *,
        market_slug: str,
        question: str,
        token_id: str,
        side: str,
        amount_usd: float,
        best_ask: float,
        midpoint: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Simulate a market buy for amount_usd at best_ask (+ slippage).

        Returns a result dict similar-ish to simmer trade result.
        """
        side = side.lower()
        if side not in ("yes", "no"):
            return {"success": False, "error": f"invalid side: {side}", "simulated": True}

        amount_usd = float(amount_usd)
        if amount_usd <= 0:
            return {"success": False, "error": "amount must be > 0", "simulated": True}

        cash = float(self._state.get("cash_usd", 0.0))
        if amount_usd > cash + 1e-9:
            return {
                "success": False,
                "error": f"insufficient paper cash: have ${cash:.2f}, need ${amount_usd:.2f}",
                "simulated": True,
            }

        # latency simulation
        if self.latency_ms > 0:
            time.sleep(self.latency_ms / 1000.0)

        fill_price = self._apply_slippage(float(best_ask), is_buy=True)
        shares = amount_usd / fill_price if fill_price > 0 else 0.0

        # Update position (avg cost)
        key = f"{market_slug}:{side}"
        pos_raw = self._state["positions"].get(key)
        if pos_raw:
            old_sh = float(pos_raw["shares"])
            old_avg = float(pos_raw["avg_entry"])
            new_sh = old_sh + shares
            new_avg = ((old_sh * old_avg) + (shares * fill_price)) / new_sh if new_sh > 0 else fill_price
        else:
            new_sh = shares
            new_avg = fill_price

        self._state["cash_usd"] = cash - amount_usd
        self._state["positions"][key] = asdict(
            PaperPosition(
                token_id=str(token_id),
                side=side,
                shares=new_sh,
                avg_entry=new_avg,
                market_slug=market_slug,
                question=question,
                updated_at=time.time(),
            )
        )
        self._save()

        row = {
            "ts": time.time(),
            "type": "buy",
            "market_slug": market_slug,
            "question": question,
            "token_id": str(token_id),
            "side": side,
            "amount_usd": round(amount_usd, 4),
            "fill_price": round(fill_price, 6),
            "shares": round(shares, 6),
            "best_ask": round(float(best_ask), 6),
            "midpoint": None if midpoint is None else round(float(midpoint), 6),
            "slippage_bps": self.slippage_bps,
            "latency_ms": self.latency_ms,
            "meta": metadata or {},
        }
        self._trade_log(row)

        return {
            "success": True,
            "trade_id": f"paper-{int(row['ts'])}",
            "shares_bought": shares,
            "shares": shares,
            "price": fill_price,
            "simulated": True,
        }


def broker_from_env(skill_dir: Path) -> PaperBroker:
    start = float(os.environ.get("PAPER_START_USD", "100"))
    slippage_bps = float(os.environ.get("PAPER_SLIPPAGE_BPS", "25"))
    latency_ms = int(float(os.environ.get("PAPER_LATENCY_MS", "350")))
    ledger_path = skill_dir / "paper_ledger.json"
    trades_path = skill_dir / "paper_trades.jsonl"
    return PaperBroker(
        ledger_path=ledger_path,
        trades_path=trades_path,
        start_usd=start,
        slippage_bps=slippage_bps,
        latency_ms=latency_ms,
    )
