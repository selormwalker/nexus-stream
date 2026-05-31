import asyncio
import websockets
import json
import logging
from .bus import NexusStream, NexusEvent

class WebSocketBridge:
    """
    Exposes the Nexus Stream over a WebSocket for distributed node connectivity.
    """
    def __init__(self, bus: NexusStream, host: str = "0.0.0.0", port: int = 8765):
        self.bus = bus
        self.host = host
        self.port = port
        self.clients = set()

    async def _handle_connection(self, websocket):
        self.clients.add(websocket)
        logging.info(f"[Bridge] Remote node connected. Total: {len(self.clients)}")
        try:
            async for message in websocket:
                data = json.loads(message)
                # Remote publish
                if "topic" in data and "data" in data:
                    await self.bus.publish(data["topic"], data["data"], metadata={"source": "remote"})
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self.clients.remove(websocket)
            logging.info(f"[Bridge] Remote node disconnected.")

    async def broadcast_loop(self):
        """Internal callback to broadcast local events to all remote clients."""
        async def forward_to_ws(event: NexusEvent):
            if event.metadata.get("source") == "remote": return
            
            message = event.model_dump_json()
            if self.clients:
                await asyncio.gather(*[client.send(message) for client in self.clients], return_exceptions=True)

        # Subscribe to all topics to forward them
        await self.bus.subscribe("*", forward_to_ws)

    async def start(self):
        logging.info(f"[Bridge] Starting WebSocket server on {self.host}:{self.port}")
        await self.broadcast_loop()
        async with websockets.serve(self._handle_connection, self.host, self.port):
            await asyncio.Future() # run forever
