// ── Droply Core Engine ────────────────────────────────────────────
// iOS Safari needs smaller chunks; other browsers can handle 256KB
const isIOS = /iPhone|iPad|iPod/.test(navigator.userAgent);
const CHUNK_SIZE = isIOS ? 64 * 1024 : 256 * 1024; // 64KB chunks
const SERVER_URL = window.location.origin;

class Droply {
  constructor() {
    this.socket = null;
    this.deviceId = null;
    this.subnet = null;
    this.trusted = false;
    this.devices = new Map();
    this.peers = new Map();
    this.channels = new Map();
    this.transfers = new Map();
    this.history = JSON.parse(localStorage.getItem('droply:history') || '[]');
    this.listeners = {};
    this.paused = new Set();
  }

  on(event, cb) {
    if (!this.listeners[event]) this.listeners[event] = [];
    this.listeners[event].push(cb);
  }

  emit(event, data) {
    (this.listeners[event] || []).forEach(cb => cb(data));
  }

  async connect(deviceName, deviceType, pin = null) {
    const roomCode = await getLocalRoomCode();
    this.socket = io(SERVER_URL);

    this.socket.on('connect', () => {
      this.socket.emit('join', { name: deviceName, type: deviceType, pin, roomCode });
    });

    this.socket.on('room:joined', ({ deviceId, roomCode, trusted, devices }) => {
      this.deviceId = deviceId;
      this.subnet = roomCode;
      this.trusted = trusted;
      devices.forEach(d => { if (d.id !== deviceId) this.devices.set(d.id, d); });
      this.emit('ready', { deviceId, subnet: roomCode, trusted, devices: [...this.devices.values()] });
    });

    this.socket.on('room:device-joined', (device) => {
      this.devices.set(device.id, device);
      this.emit('device-joined', device);
    });

    this.socket.on('room:device-left', ({ id }) => {
      this.devices.delete(id);
      this.peers.delete(id);
      this.emit('device-left', { id });
    });

    this.socket.on('signal:offer', async ({ fromId, offer, transferMeta }) => {
      await this._handleOffer(fromId, offer, transferMeta);
    });

    this.socket.on('signal:answer', async ({ fromId, answer }) => {
      const pc = this.peers.get(fromId);
      if (pc) await pc.setRemoteDescription(new RTCSessionDescription(answer));
    });

    this.socket.on('signal:ice', async ({ fromId, candidate }) => {
      const pc = this.peers.get(fromId);
      if (pc && candidate) await pc.addIceCandidate(new RTCIceCandidate(candidate));
    });

    this.socket.on('transfer:accepted', ({ fromId, transferId }) => {
      this.emit('transfer-accepted', { fromId, transferId });
      this._startSending(fromId, transferId);
    });

    this.socket.on('transfer:rejected', ({ fromId, transferId }) => {
      this.transfers.delete(transferId);
      this.emit('transfer-rejected', { fromId, transferId });
    });

    this.socket.on('transfer:progress', ({ fromId, transferId, progress }) => {
      this.emit('transfer-progress-remote', { fromId, transferId, progress });
    });
  }

  async sendFile(targetId, file) {
    const transferId = crypto.randomUUID();
    const chunkCount = Math.ceil(file.size / CHUNK_SIZE);

    this.transfers.set(transferId, {
      id: transferId, targetId, file, chunkCount,
      sentChunks: 0, status: 'pending', direction: 'out',
    });

    const transferMeta = {
      transferId, fileName: file.name,
      fileSize: file.size, fileType: file.type, chunkCount,
    };

    const pc = await this._createPeer(targetId);
    const channel = pc.createDataChannel(`transfer:${transferId}`, { ordered: true });
    this.channels.set(`${targetId}:${transferId}`, channel);
    this._setupSendChannel(channel, transferId);

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    this.socket.emit('signal:offer', { targetId, offer, transferMeta });

    return transferId;
  }

  pauseTransfer(transferId) {
    this.paused.add(transferId);
    this.emit('transfer-paused', { transferId });
  }

  resumeTransfer(transferId) {
    this.paused.delete(transferId);
    const transfer = this.transfers.get(transferId);
    if (transfer) this._startSending(transfer.targetId, transferId);
    this.emit('transfer-resumed', { transferId });
  }

  acceptTransfer(fromId, transferId) {
    this.socket.emit('transfer:accept', { targetId: fromId, transferId });
  }

  rejectTransfer(fromId, transferId) {
    this.socket.emit('transfer:reject', { targetId: fromId, transferId });
  }

  setPin(pin) {
    this.socket.emit('room:set-pin', { pin });
  }

  getHistory() { return this.history; }

  _addHistory(entry) {
    this.history.unshift(entry);
    if (this.history.length > 100) this.history.pop();
    localStorage.setItem('droply:history', JSON.stringify(this.history));
    this.emit('history-updated', this.history);
  }

  async _createPeer(peerId) {
    if (this.peers.has(peerId)) return this.peers.get(peerId);

    const pc = new RTCPeerConnection({
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun2.l.google.com:19302' },
      ],
    });

    pc.onicecandidate = ({ candidate }) => {
      if (candidate) this.socket.emit('signal:ice', { targetId: peerId, candidate });
    };

    pc.ondatachannel = (event) => {
      this._setupReceiveChannel(event.channel, peerId);
    };

    this.peers.set(peerId, pc);
    return pc;
  }

  async _handleOffer(fromId, offer, transferMeta) {
    this.transfers.set(transferMeta.transferId, {
      id: transferMeta.transferId, fromId, meta: transferMeta,
      chunks: new Array(transferMeta.chunkCount),
      receivedChunks: 0, status: 'pending', direction: 'in',
    });

    const pc = await this._createPeer(fromId);
    await pc.setRemoteDescription(new RTCSessionDescription(offer));
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    this.socket.emit('signal:answer', { targetId: fromId, answer });

    // Auto-accept: notify UI then immediately send accept signal
    this.emit('transfer-incoming', { fromId, transferMeta });
    this.socket.emit('transfer:accept', { targetId: fromId, transferId: transferMeta.transferId });
  }

  _setupSendChannel(channel, transferId) {
    channel.onerror = (e) => {
      console.error('Send channel error', e);
      this.emit('transfer-error', { transferId });
    };
  }

  async _startSending(targetId, transferId) {
    const transfer = this.transfers.get(transferId);
    if (!transfer) return;

    const channelKey = `${targetId}:${transferId}`;
    const channel = this.channels.get(channelKey);
    if (!channel || channel.readyState !== 'open') {
      // Wait for channel to open
      const ch = this.channels.get(channelKey);
      if (ch) {
        ch.onopen = () => this._startSending(targetId, transferId);
      }
      return;
    }

    const { file, chunkCount } = transfer;
    let offset = transfer.sentChunks * CHUNK_SIZE;
    transfer.status = 'sending';
    this.emit('transfer-started', { transferId, direction: 'out' });

    const sendNext = async () => {
      if (this.paused.has(transferId)) return;
      if (transfer.sentChunks >= chunkCount) {
        channel.send(JSON.stringify({ type: 'done', transferId }));
        transfer.status = 'done';
        this._addHistory({
          id: transferId, direction: 'out', fileName: file.name,
          fileSize: file.size, targetId, timestamp: Date.now(),
        });
        this.emit('transfer-complete', { transferId, direction: 'out', fileName: file.name });
        return;
      }

      // iOS Safari has a much smaller buffer limit
      const bufferLimit = isIOS ? 128 * 1024 : 1024 * 1024;
      if (channel.bufferedAmount > bufferLimit) {
        setTimeout(sendNext, isIOS ? 20 : 50);
        return;
      }

      const slice = file.slice(offset, offset + CHUNK_SIZE);
      const buffer = await slice.arrayBuffer();

      channel.send(JSON.stringify({ type: 'chunk', transferId, index: transfer.sentChunks, total: chunkCount }));
      channel.send(buffer);

      transfer.sentChunks++;
      offset += CHUNK_SIZE;

      const progress = Math.round((transfer.sentChunks / chunkCount) * 100);
      this.emit('transfer-progress', { transferId, progress, direction: 'out' });
      this.socket.emit('transfer:progress', { targetId, transferId, progress });

      setTimeout(sendNext, 0);
    };

    sendNext();
  }

  _setupReceiveChannel(channel, fromId) {
    let currentMeta = null;
    let currentTransfer = null;
    let expectingBinary = false;

    channel.onmessage = async (event) => {
      if (typeof event.data === 'string') {
        const msg = JSON.parse(event.data);

        if (msg.type === 'chunk') {
          currentMeta = msg;
          currentTransfer = this.transfers.get(msg.transferId);
          expectingBinary = true;

          if (currentTransfer && currentTransfer.status === 'pending') {
            currentTransfer.status = 'receiving';
            this.emit('transfer-started', { transferId: msg.transferId, direction: 'in' });
          }
        }

        if (msg.type === 'done') {
          const transfer = this.transfers.get(msg.transferId);
          if (!transfer) return;

          const blob = new Blob(transfer.chunks, { type: transfer.meta.fileType });
          const url = URL.createObjectURL(blob);

          this._addHistory({
            id: msg.transferId, direction: 'in',
            fileName: transfer.meta.fileName,
            fileSize: transfer.meta.fileSize,
            fromId, timestamp: Date.now(), url,
          });

          transfer.status = 'done';
          this.emit('transfer-complete', {
            transferId: msg.transferId, direction: 'in',
            fileName: transfer.meta.fileName,
            fileSize: transfer.meta.fileSize,
            url, blob,
          });
        }
      } else if (expectingBinary && currentMeta && currentTransfer) {
        currentTransfer.chunks[currentMeta.index] = event.data;
        currentTransfer.receivedChunks++;
        expectingBinary = false;

        const progress = Math.round((currentTransfer.receivedChunks / currentTransfer.meta.chunkCount) * 100);
        this.emit('transfer-progress', { transferId: currentMeta.transferId, progress, direction: 'in' });
      }
    };

    channel.onerror = (e) => console.error('Receive channel error', e);
  }
}

window.Droply = Droply;

// ── Room code detection ───────────────────────────────────────────
// Gets local IP via WebRTC and derives a room code from the subnet
async function getLocalRoomCode() {
  return new Promise((resolve) => {
    const pc = new RTCPeerConnection({ iceServers: [] });
    pc.createDataChannel('');
    pc.createOffer().then(o => pc.setLocalDescription(o));
    pc.onicecandidate = (e) => {
      if (!e || !e.candidate) return;
      const match = e.candidate.candidate.match(/(\d+\.\d+\.\d+)\.\d+/);
      pc.close();
      if (match) {
        // Hash the subnet into a short room code
        const subnet = match[1];
        let hash = 0;
        for (let i = 0; i < subnet.length; i++) {
          hash = ((hash << 5) - hash) + subnet.charCodeAt(i);
          hash |= 0;
        }
        resolve('room-' + Math.abs(hash).toString(36));
      } else {
        resolve('room-local');
      }
    };
    setTimeout(() => resolve('room-local'), 3000);
  });
}

window.getLocalRoomCode = getLocalRoomCode;
