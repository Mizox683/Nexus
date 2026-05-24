const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);

// ── Persistence ───────────────────────────────────────────────────
const DB_PATH = path.join('/tmp', 'nexus-devices.json');

function saveDevices() {
  try {
    const data = {};
    registeredDevices.forEach((v, k) => { data[k] = v; });
    fs.writeFileSync(DB_PATH, JSON.stringify(data));
  } catch(e) { console.error('Save error:', e.message); }
}

function loadDevices() {
  try {
    if (fs.existsSync(DB_PATH)) {
      const data = JSON.parse(fs.readFileSync(DB_PATH, 'utf8'));
      Object.entries(data).forEach(([k, v]) => {
        v.socketId = null; // all offline on restart
        registeredDevices.set(k, v);
      });
      console.log(`[db] Loaded ${registeredDevices.size} registered devices`);
    }
  } catch(e) { console.error('Load error:', e.message); }
}
const io = new Server(server, {
  cors: { origin: '*' },
  maxHttpBufferSize: 1e8
});

app.use(express.json());
app.get('/socket.io/socket.io.js', (req, res) => {
  res.sendFile(require.resolve('socket.io/client-dist/socket.io.js'));
});
app.use(express.static(path.join(__dirname, '../client/public')));

// ── State ─────────────────────────────────────────────────────────
const rooms = new Map();
// registeredDevices: { [deviceKey]: { name, type, roomCode, pin, socketId|null } }
const registeredDevices = new Map();
// pendingFiles: { [deviceKey]: [ {fromName, fileName, fileSize, chunks, meta} ] }
const pendingFiles = new Map();

loadDevices();

function roomKey(code) { return code; }

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, { devices: new Map(), pin: null });
  return rooms.get(code);
}

function getRoomDeviceList(code) {
  return Array.from(getRoom(code).devices.values()).map(d => ({
    id: d.id, name: d.name, type: d.type, trusted: d.trusted,
  }));
}

// Device key = roomCode + deviceName (persistent identity)
function devKey(roomCode, name) { return roomCode + ':' + name; }

io.on('connection', (socket) => {
  let currentRoom = null;
  let currentDevKey = null;

  socket.on('join', ({ name, type, pin, roomCode }) => {
    const code = roomCode || 'default';
    currentRoom = code;
    const room = getRoom(code);

    // PIN check
    if (!room.pin) {
      if (!pin) { socket.emit('room:rejected', { reason: 'PIN required' }); return; }
      room.pin = pin;
    }
    if (room.pin !== pin) { socket.emit('room:rejected', { reason: 'Wrong PIN' }); return; }

    const trusted = true;
    const device = { id: socket.id, name: name || 'Unknown', type: type || 'unknown', trusted };
    room.devices.set(socket.id, device);
    socket.join(code);

    // Register device persistently
    currentDevKey = devKey(code, name);
    registeredDevices.set(currentDevKey, { name, type, roomCode: code, pin, socketId: socket.id });
    saveDevices();

    socket.emit('room:joined', { deviceId: socket.id, roomCode: code, trusted, devices: getRoomDeviceList(code) });
    socket.to(code).emit('room:device-joined', { id: socket.id, name, type, trusted });

    // Deliver any queued files
    const queue = pendingFiles.get(currentDevKey) || [];
    if (queue.length > 0) {
      queue.forEach(pf => {
        socket.emit('queued:file', pf);
      });
      pendingFiles.delete(currentDevKey);
    }

    console.log(`[+] ${name} joined room:${code}`);
  });

  // WebRTC signaling
  socket.on('signal:offer', ({ targetId, offer, transferMeta }) => {
    io.to(targetId).emit('signal:offer', { fromId: socket.id, offer, transferMeta });
  });
  socket.on('signal:answer', ({ targetId, answer }) => {
    io.to(targetId).emit('signal:answer', { fromId: socket.id, answer });
  });
  socket.on('signal:ice', ({ targetId, candidate }) => {
    io.to(targetId).emit('signal:ice', { fromId: socket.id, candidate });
  });
  socket.on('transfer:accept', ({ targetId, transferId }) => {
    io.to(targetId).emit('transfer:accepted', { fromId: socket.id, transferId });
  });
  socket.on('transfer:reject', ({ targetId, transferId }) => {
    io.to(targetId).emit('transfer:rejected', { fromId: socket.id, transferId });
  });
  socket.on('transfer:progress', ({ targetId, transferId, progress }) => {
    io.to(targetId).emit('transfer:progress', { fromId: socket.id, transferId, progress });
  });

  // Queue file for offline device
  socket.on('queue:file', ({ targetDevKey, fromName, fileName, fileSize, fileType, fileData }) => {
    // Only queue small files (under 50MB) to avoid memory issues
    if (fileSize > 50 * 1024 * 1024) {
      socket.emit('queue:error', { reason: 'File too large to queue (max 50MB for offline delivery)' });
      return;
    }
    if (!pendingFiles.has(targetDevKey)) pendingFiles.set(targetDevKey, []);
    pendingFiles.get(targetDevKey).push({ fromName, fileName, fileSize, fileType, fileData, ts: Date.now() });
    socket.emit('queue:ok', { targetDevKey });
    console.log(`[queue] ${fromName} -> ${targetDevKey}: ${fileName}`);
  });

  // Text messages
  socket.on('remote:command', ({ targetId, cmd, data }) => {
    io.to(targetId).emit('remote:command', { fromId: socket.id, cmd, data });
  });

  socket.on('remote:screen', ({ targetId, frame }) => {
    io.to(targetId).emit('remote:screen', { frame });
  });

  socket.on('remote:input', ({ targetId, type, data }) => {
    io.to(targetId).emit('remote:input', { type, data });
  });

  socket.on('text:send', ({ targetId, text, msgId }) => {
    io.to(targetId).emit('text:receive', { fromId: socket.id, text, msgId });
  });

  // Get registered devices in room (including offline ones)
  socket.on('room:get-registered', ({ roomCode, pin }) => {
    const registered = [];
    registeredDevices.forEach((dev, key) => {
      if (dev.roomCode === roomCode && dev.pin === pin) {
        const online = io.sockets.adapter.rooms.get(roomCode)?.has(dev.socketId);
        registered.push({ devKey: key, name: dev.name, type: dev.type, online: !!online });
      }
    });
    socket.emit('room:registered', { devices: registered });
  });

  socket.on('disconnect', () => {
    if (!currentRoom) return;
    const room = getRoom(currentRoom);
    const device = room.devices.get(socket.id);
    if (device) {
      room.devices.delete(socket.id);
      socket.to(currentRoom).emit('room:device-left', { id: socket.id });
      console.log(`[-] ${device.name} left room:${currentRoom}`);
    }
    // Keep room alive, don't delete registered devices
    if (room.devices.size === 0 && !registeredDevices.size) rooms.delete(currentRoom);
  });
});

app.get('/health', (_, res) => res.json({ ok: true, rooms: rooms.size, registered: registeredDevices.size }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Nexus running on port ${PORT}`));
