"""Run after cargo build. Exercises offline reads, restart, and client handoff."""
import asyncio
import sqlite3
import tempfile
from collections import deque
from pathlib import Path

from fake_radio import FakeRadio, encode_response, make_chan_msg, make_contact_msg, make_messages_waiting
from test_proxy import connect_client, find_free_port, wait_for_port


class InboxRadio(FakeRadio):
    def __init__(self):
        super().__init__()
        self.inbox = deque()
        self.writer = None
        self.pop_started = asyncio.Event()
        self.allow_reply = asyncio.Event()
        self.allow_reply.set()

    async def _handle_command(self, payload, w):
        self.writer = w
        if payload == b"\x0a":
            if self.inbox:
                frame = self.inbox.popleft()
                self.pop_started.set()
                await self.allow_reply.wait()
            else:
                frame = encode_response(b"\x0a")
            w.write(frame)
            await w.drain()
        else:
            await super()._handle_command(payload, w)


async def scenario(directory):
    radio = InboxRadio()
    server = await asyncio.start_server(radio.handle_client, "127.0.0.1", 0)
    backend_port = server.sockets[0].getsockname()[1]
    port = await find_free_port()

    async def start():
        return await asyncio.create_subprocess_exec(
            Path(__file__).resolve().parents[1] / "target/debug/coresplitter",
            "--tcp-backend-host", "127.0.0.1", "--tcp-backend-port", str(backend_port),
            "--tcp-frontend-port", str(port), "--data-dir", directory, "--drain-offline",
        )

    async def phone():
        c = await connect_client(port=port)
        assert await c.appstart("MeshCore Open")
        return c

    async def stop(proc):
        proc.terminate()
        await asyncio.wait_for(proc.wait(), 5)

    async with server:
        proc = await start()
        try:
            assert await wait_for_port(port)
            c = await phone()
            await c.close()
            await c.w.wait_closed()

            # No notification: the fallback poll must discover the pending batch.
            frames = [make_chan_msg(0, f"offline-{n}", True) for n in range(300)]
            frames.append(make_contact_msg(b"abcdef", "offline DM", True))
            binary = encode_response(bytes.fromhex("1b 04 00 00 00 ff 01 00 01 ab"))
            frames.append(binary)
            radio.inbox.extend(frames)
            with sqlite3.connect(Path(directory) / "state.db") as db:
                async with asyncio.timeout(20):
                    while db.execute("SELECT count(*) FROM messages").fetchone()[0] < len(frames):
                        await asyncio.sleep(0.05)
                assert db.execute("SELECT frame FROM messages ORDER BY id").fetchall() == [
                    (frame[3:],) for frame in frames
                ]

            await stop(proc)
            proc = await start()
            assert await wait_for_port(port)
            c = await phone()
            for n in range(300):
                assert (await c.get_message())[8:] == f"offline-{n}".encode()
            assert (await c.get_message()).endswith(b"offline DM")
            await c.send(b"\x0a")
            assert await c.recv() == binary[3:]
            assert await c.get_message() == b"\x0a"
            await c.close()
            await c.w.wait_closed()

            # Hold the radio's reply while the phone starts its next session.
            radio.allow_reply.clear()
            radio.pop_started.clear()
            radio.inbox.append(make_chan_msg(0, "handoff", True))
            radio.writer.write(make_messages_waiting())
            await asyncio.wait_for(radio.pop_started.wait(), 15)
            c = await connect_client(port=port)
            handshake = asyncio.create_task(c.appstart("MeshCore Open"))
            await asyncio.sleep(0.05)
            radio.allow_reply.set()
            assert await handshake
            assert (await c.get_message()).endswith(b"handoff")
            assert await c.get_message() == b"\x0a"
            assert await c.get_channel(0)
            await c.close()
        finally:
            if proc.returncode is None:
                await stop(proc)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        asyncio.run(scenario(directory))
    print("Offline inbox, restart and reconnect handoff passed")
