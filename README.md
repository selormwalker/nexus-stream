# ðŸŒŠ Nexus Stream

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/release/python-380/)
[![Performance: High-Velocity](https://img.shields.io/badge/performance-high--velocity-orange.svg)](#)

**Nexus Stream** is a lightweight, high-velocity asynchronous event bus designed for distributed AI systems and microservices. Built on top of Python's `asyncio`, it provides a clean and performant publish-subscribe (PubSub) pattern for decoupled system communication.

## ðŸš€ Features
- **Asynchronous Core:** Non-blocking event loop using `asyncio`.
- **Concurrent Callbacks:** Executes subscriber callbacks concurrently for maximum throughput.
- **Dynamic Topics:** Subscribe and publish to any string-based topic.
- **Minimal Footprint:** Zero external dependencies (standard library only).

## ðŸ’» Quick Start
```python
import asyncio
from nexus_stream import NexusStream

async def main():
    stream = NexusStream()
    
    # Define a subscriber
    async def handler(data):
        print(f"Message received: {data}")

    # Subscribe to a topic
    await stream.subscribe("sensor_data", handler)
    
    # Start the stream
    asyncio.create_task(stream.start())
    
    # Publish an event
    await stream.publish("sensor_data", {"temperature": 22.5})
    
    await asyncio.sleep(0.1)

if __name__ == "__main__":
    asyncio.run(main())
```

## ðŸ¤ Contributing
Help us make distributed events even faster. Issues and pull requests are welcome.

---
Built with ðŸ§  by [David Selorm Walker](https://github.com/selormwalker)
