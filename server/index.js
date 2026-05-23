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
app.use(express.static(path.join(__dirname, '../client/public')));

const rooms = new Map();

function getSubnet(ip) {
  const clean = ip.replace('::ffff:', '');
  if (clean === '::1' || clean === '127.0.0.1') return 'localhost';
  const parts = clean.split('.');
  if (parts.length === 4) return parts.slice(0, 3).join('.');
  return clean;
}

function getRoom(subnet) {
  if (!rooms.has(subnet)) rooms.set(subnet, { devices: new Map(), pin: null });
  return rooms.get(subnet);
}

function getRoomDeviceList(subnet) {
  const room = getRoom(subnet);
  return Array.from(room.devices.values()).map(d => ({
    id: d.id, name: d.name, type: d.type, trusted: d.trusted,
  }));
}

io.on('connection', (socket) => {
  const rawIp = socket.handshake.headers['x-forwarded-for'] || socket.handshake.address;
  const ip = rawIp.split(',')[0].trim();
  const subnet = getSubnet(ip);

  console.log(`[+] ${socket.id} connected from ${ip} (subnet: ${subnet})`);

  socket.on('join', ({ name, type, pin }) => {
    const room = getRoom(subnet);
    const trusted = room.pin ? room.pin === pin : true;

    const device = { id: socket.id, name: name || 'Unknown Device', type: type || 'unknown', trusted, subnet, ip };
    room.devices.set(socket.id, device);
    socket.join(subnet);

    socket.emit('room:joined', { deviceId: socket.id, subnet, trusted, devices: getRoomDeviceList(subnet) });
    socket.to(subnet).emit('room:device-joined', { id: device.id, name: device.name, type: device.type, trusted: device.trusted });

    console.log(`[room] ${device.name} joined subnet ${subnet} (trusted: ${trusted})`);
  });

  socket.on('room:set-pin', ({ pin }) => {
    const room = getRoom(subnet);
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

  socket.on('transfer:progress', ({ targetId, transferId, progress }) => {
    io.to(targetId).emit('transfer:progress', { fromId: socket.id, transferId, progress });
  });

  socket.on('disconnect', () => {
    const room = getRoom(subnet);
    const device = room.devices.get(socket.id);
    if (device) {
      room.devices.delete(socket.id);
      socket.to(subnet).emit('room:device-left', { id: socket.id });
      console.log(`[-] ${device.name} left subnet ${subnet}`);
    }
    if (room.devices.size === 0) rooms.delete(subnet);
  });
});

app.get('/health', (_, res) => res.json({ ok: true, rooms: rooms.size }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Droply server running on port ${PORT}`));
