import asyncio
import logging
from nexus_stream.bus import NexusStream, RedisBackend
from nexus_stream.bridge import WebSocketBridge

async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    
    # Initialize with Redis durability if available
    backend = RedisBackend()
    bus = NexusStream(backend=backend)
    
    # Start the Bridge for remote connectivity
    bridge = WebSocketBridge(bus)
    
    async def on_all_events(event):
        logging.info(f"[Monitor] Observed: {event.topic} -> {event.data}")

    # Catch-all subscription
    await bus.subscribe("*", on_all_events)
    
    # Run the system
    logging.info("Nexus Stream distributed node starting...")
    await asyncio.gather(
        bus.start(),
        bridge.start()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
