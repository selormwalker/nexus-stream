import asyncio
import json
import logging
from collections import defaultdict
from typing import Callable, Any, Dict, List

class NexusStream:
    """
    A high-velocity asynchronous event bus.
    Allows for decoupled communication between distributed AI agents
    or microservices using a publish-subscribe pattern.
    """
    def __init__(self):
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.queue = asyncio.Queue()
        self._running = True

    async def subscribe(self, topic: str, callback: Callable[[Any], Any]):
        """Register a callback for a specific topic."""
        self.subscribers[topic].append(callback)
        logging.info(f"Subscribed to topic: {topic}")

    async def publish(self, topic: str, data: Any):
        """Publish an event to a topic."""
        await self.queue.put((topic, data))

    async def start(self):
        """Start the event processing loop."""
        logging.info("Nexus Stream event loop started.")
        while self._running:
            topic, data = await self.queue.get()
            if topic in self.subscribers:
                # Execute callbacks concurrently
                tasks = [asyncio.create_task(cb(data)) for callback in self.subscribers[topic]]
                if tasks:
                    await asyncio.gather(*tasks)
            self.queue.task_done()

    def stop(self):
        """Stop the event loop."""
        self._running = False
        logging.info("Nexus Stream stopping...")

# Example Usage
async def main():
    stream = NexusStream()
    
    async def on_message(data):
        print(f"Received data: {data}")

    await stream.subscribe("ai_updates", on_message)
    
    # Run stream in background
    stream_task = asyncio.create_task(stream.start())
    
    # Publish some events
    await stream.publish("ai_updates", {"status": "processing", "agent": "Alpha-1"})
    await stream.publish("ai_updates", {"status": "complete", "agent": "Alpha-1"})
    
    await asyncio.sleep(1)
    stream.stop()
    await stream_task

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
