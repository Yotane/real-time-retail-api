import threading
from dataclasses import dataclass, field
from typing import Optional
from datetime import date


@dataclass
class SimulationState:
    """
    Shared state between FastAPI and the producer.
    Thread-safe via a lock.
    """
    all_dates:       list        = field(default_factory=list)
    current_index:   int         = -1
    current_day:     Optional[str] = None
    day_total:       int         = 0
    day_received:    int         = 0
    is_running:      bool        = False
    is_complete:     bool        = False
    _lock:           threading.Lock = field(default_factory=threading.Lock)

    def advance_day(self) -> Optional[str]:
        with self._lock:
            if self.current_index + 1 >= len(self.all_dates):
                self.is_complete = True
                return None
            self.current_index += 1
            self.current_day    = str(self.all_dates[self.current_index])
            self.day_total      = 0
            self.day_received   = 0
            self.is_running     = True
            return self.current_day

    def set_day_total(self, total: int):
        with self._lock:
            self.day_total = total

    def increment_received(self):
        with self._lock:
            self.day_received += 1

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "current_day":   self.current_day,
                "day_index":     self.current_index,
                "total_days":    len(self.all_dates),
                "day_total":     self.day_total,
                "day_received":  self.day_received,
                "is_running":    self.is_running,
                "is_complete":   self.is_complete,
                "progress_pct":  round(
                    (self.day_received / self.day_total * 100)
                    if self.day_total > 0 else 0, 1
                )
            }


# Single shared instance — imported by main.py, producer, consumer
simulation = SimulationState()