const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  maxHttpBufferSize: 1e8
});

app.use(express.json());

app.get('/socket.io/socket.io.js', (req, res) => {
  res.sendFile(require.resolve('socket.io/client-dist/socket.io.js'));
});

app.use(express.static(path.join(__dirname, '../client/public')));

// rooms: { [roomCode]: { devices: Map<socketId, deviceInfo> , pin: string|null } }
const rooms = new Map();

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, { devices: new Map(), pin: null });
  return rooms.get(code);
}

function getRoomDeviceList(code) {
  return Array.from(getRoom(code).devices.values()).map(d => ({
    id: d.id, name: d.name, type: d.type, trusted: d.trusted,
  }));
}

io.on('connection', (socket) => {
  let currentRoom = null;

  console.log(`[+] ${socket.id} connected`);

  // Client sends its local network room code (derived from local IP in browser)
  socket.on('join', ({ name, type, pin, roomCode }) => {
    const code = roomCode || 'default';
    currentRoom = code;

    const room = getRoom(code);
    const trusted = room.pin ? room.pin === pin : true;

    const device = { id: socket.id, name: name || 'Unknown Device', type: type || 'unknown', trusted };
    room.devices.set(socket.id, device);
    socket.join(code);

    socket.emit('room:joined', { deviceId: socket.id, roomCode: code, trusted, devices: getRoomDeviceList(code) });
    socket.to(code).emit('room:device-joined', { id: device.id, name: device.name, type: device.type, trusted: device.trusted });

    console.log(`[room:${code}] ${device.name} joined (trusted: ${trusted})`);
  });

  socket.on('room:set-pin', ({ pin }) => {
    if (!currentRoom) return;
    const room = getRoom(currentRoom);
    const device = room.devices.get(socket.id);
    if (!device || !device.trusted) return socket.emit('error', { msg: 'Not trusted' });
    room.pin = pin;
    socket.emit('room:pin-set', { ok: true });
  });

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

  socket.on('text:send', ({ targetId, text, msgId }) => {
    io.to(targetId).emit('text:receive', { fromId: socket.id, text, msgId });
  });

  socket.on('transfer:progress', ({ targetId, transferId, progress }) => {
    io.to(targetId).emit('transfer:progress', { fromId: socket.id, transferId, progress });
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
    if (room.devices.size === 0) rooms.delete(currentRoom);
  });
});

app.get('/health', (_, res) => res.json({ ok: true, rooms: rooms.size }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Nexus server running on port ${PORT}`));
