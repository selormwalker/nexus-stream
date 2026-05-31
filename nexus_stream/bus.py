import asyncio
import json
import logging
import time
import fnmatch
from collections import defaultdict
from typing import Callable, Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field
import redis

# --- Core Models ---

class NexusEvent(BaseModel):
    topic: str
    data: Any
    timestamp: float = Field(default_factory=time.time)
    metadata: Dict[str, Any] = Field(default_factory=dict)

# --- Durable Backends ---

class BaseBackend:
    def store(self, event: NexusEvent): pass
    def retrieve(self, topic: str) -> List[NexusEvent]: return []

class RedisBackend(BaseBackend):
    def __init__(self, host='localhost', port=6379):
        try:
            self.client = redis.Redis(host=host, port=port, decode_responses=True)
            self.client.ping()
        except:
            logging.warning("Redis backend unavailable. Falling back to transient.")
            self.client = None

    def store(self, event: NexusEvent):
        if self.client:
            self.client.rpush(f"nexus:stream:{event.topic}", event.model_dump_json())

class NexusStream:
    def __init__(self, backend: Optional[BaseBackend] = None):
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.queue = asyncio.Queue()
        self.backend = backend
        self._running = True
        
        # Telemetry
        self.msg_count = 0
        self.start_time = time.time()

    async def subscribe(self, pattern: str, callback: Callable[[NexusEvent], Any]):
        """Register a callback using wildcard patterns (e.g., 'ai.*', 'system.>')."""
        self.subscribers[pattern].append(callback)
        logging.info(f"[Nexus] New subscription: {pattern}")

    async def publish(self, topic: str, data: Any, metadata: Optional[Dict] = None):
        """Publish an event to the bus."""
        event = NexusEvent(topic=topic, data=data, metadata=metadata or {})
        
        # Persist if backend exists
        if self.backend:
            self.backend.store(event)
            
        await self.queue.put(event)

    def _get_matching_subscribers(self, topic: str) -> List[Callable]:
        matches = []
        for pattern, callbacks in self.subscribers.items():
            # Support both * and > (standard messaging glob)
            norm_pattern = pattern.replace('>', '*')
            if fnmatch.fnmatch(topic, norm_pattern):
                matches.extend(callbacks)
        return matches

    async def start(self):
        """High-velocity event processing loop."""
        logging.info("[Nexus] Event processing engine online.")
        while self._running:
            event = await self.queue.get()
            self.msg_count += 1
            
            callbacks = self._get_matching_subscribers(event.topic)
            if callbacks:
                tasks = [asyncio.create_task(cb(event)) for cb in callbacks]
                await asyncio.gather(*tasks, return_exceptions=True)
            
            self.queue.task_done()

    def get_stats(self):
        uptime = time.time() - self.start_time
        return {
            "uptime_sec": round(uptime, 2),
            "total_messages": self.msg_count,
            "avg_throughput": round(self.msg_count / uptime, 2) if uptime > 0 else 0
        }

    def stop(self):
        self._running = False
        logging.info("[Nexus] Stream shutting down...")
