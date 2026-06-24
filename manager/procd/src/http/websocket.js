import crypto from "node:crypto";
import { EventEmitter } from "node:events";

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

export class WebSocketConnection extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.closed = false;
    socket.on("data", (chunk) => this.onData(chunk));
    socket.on("close", () => this.emit("close"));
    socket.on("error", (err) => this.emit("error", err));
  }

  onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (true) {
      const frame = this.readFrame();
      if (!frame) return;
      if (frame.opcode === 0x8) {
        this.close();
        return;
      }
      if (frame.opcode === 0x9) {
        this.writeFrame(0xA, frame.payload);
        continue;
      }
      if (frame.opcode === 0xA) continue;
      if (frame.opcode === 0x1) {
        this.emit("message", "text", frame.payload.toString("utf8"));
      } else if (frame.opcode === 0x2) {
        this.emit("message", "binary", frame.payload);
      }
    }
  }

  readFrame() {
    if (this.buffer.length < 2) return null;
    const first = this.buffer[0];
    const second = this.buffer[1];
    const opcode = first & 0x0f;
    const masked = (second & 0x80) !== 0;
    let length = second & 0x7f;
    let offset = 2;
    if (length === 126) {
      if (this.buffer.length < offset + 2) return null;
      length = this.buffer.readUInt16BE(offset);
      offset += 2;
    } else if (length === 127) {
      if (this.buffer.length < offset + 8) return null;
      const hi = this.buffer.readUInt32BE(offset);
      const lo = this.buffer.readUInt32BE(offset + 4);
      length = hi * 2 ** 32 + lo;
      offset += 8;
    }
    let mask;
    if (masked) {
      if (this.buffer.length < offset + 4) return null;
      mask = this.buffer.subarray(offset, offset + 4);
      offset += 4;
    }
    if (this.buffer.length < offset + length) return null;
    let payload = this.buffer.subarray(offset, offset + length);
    this.buffer = this.buffer.subarray(offset + length);
    if (masked) {
      payload = Buffer.from(payload);
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
    }
    return { opcode, payload };
  }

  sendJSON(value) {
    this.sendText(JSON.stringify(value));
  }

  sendText(value) {
    this.writeFrame(0x1, Buffer.from(String(value)));
  }

  sendBinary(value) {
    this.writeFrame(0x2, Buffer.isBuffer(value) ? value : Buffer.from(value));
  }

  close(reason = "") {
    if (this.closed) return;
    this.closed = true;
    const payload = reason ? Buffer.concat([Buffer.from([0x03, 0xe8]), Buffer.from(reason)]) : Buffer.alloc(0);
    try {
      this.writeFrame(0x8, payload);
    } catch {
      // Socket is already closing.
    }
    this.socket.end();
  }

  writeFrame(opcode, payload) {
    if (this.closed && opcode !== 0x8) return;
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.from([0x80 | opcode, len]);
    } else if (len <= 0xffff) {
      header = Buffer.alloc(4);
      header[0] = 0x80 | opcode;
      header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x80 | opcode;
      header[1] = 127;
      header.writeUInt32BE(Math.floor(len / 2 ** 32), 2);
      header.writeUInt32BE(len >>> 0, 6);
    }
    this.socket.write(Buffer.concat([header, payload]));
  }
}

export function isWebSocketUpgrade(req) {
  return String(req.headers.upgrade || "").toLowerCase() === "websocket";
}

export function acceptWebSocket(req, socket, head) {
  const key = req.headers["sec-websocket-key"];
  if (!key) {
    socket.destroy();
    return null;
  }
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write([
    "HTTP/1.1 101 Switching Protocols",
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Accept: ${accept}`,
    "",
    ""
  ].join("\r\n"));
  const conn = new WebSocketConnection(socket);
  if (head && head.length) conn.onData(head);
  return conn;
}
