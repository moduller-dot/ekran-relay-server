const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*" },
  maxHttpBufferSize: 1e8,
  perMessageDeflate: false
});

const PORT = process.env.PORT || 10000;
const rooms = {};
const disconnectTimers = {};

const MASTER_GRACE_MS = 15000;

io.on('connection', (socket) => {
  console.log('[SERVER] Device connected:', socket.id);

  socket.on('create-room', (roomId) => {
    socket.join(roomId);
    socket.roomId = roomId;
    socket.isMaster = true;

    const timeString = new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

    if (disconnectTimers[roomId]) {
      clearTimeout(disconnectTimers[roomId]);
      delete disconnectTimers[roomId];
      console.log(`[SERVER] Master reconnected (create-room), room preserved: ${roomId}`);
    }

    if (rooms[roomId] && rooms[roomId].pendingMasterSocketId) {
      rooms[roomId].master = socket.id;
      rooms[roomId].pendingMasterSocketId = null;
      const masterEntry = rooms[roomId].clients.find(c => c.role === 'MASTER');
      if (masterEntry) {
        masterEntry.socketId = socket.id;
      }
    } else {
      rooms[roomId] = {
        master: socket.id,
        clients: [{
          socketId: socket.id,
          deviceId: 'Master Device',
          role: 'MASTER',
          connectedAt: timeString
        }],
        slotAssignments: {},
        isStreaming: false,
        pendingMasterSocketId: null,
        streamWidth: null,
        streamHeight: null,
        soundConfig: null
      };
    }

    io.to(roomId).emit('room:update', {
      count: rooms[roomId].clients.length,
      clients: rooms[roomId].clients
    });
  });

  socket.on('join-room', (data) => {
    const roomId = data.roomId;
    const isMaster = data.isMaster;
    const deviceId = data.deviceId || socket.id;

    if (!rooms[roomId]) return;

    socket.join(roomId);
    socket.roomId = roomId;
    socket.isMaster = isMaster;

    const timeString = new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

    if (isMaster === true) {
      if (disconnectTimers[roomId]) {
        clearTimeout(disconnectTimers[roomId]);
        delete disconnectTimers[roomId];
        console.log(`[SERVER] Master reconnected (join-room), room preserved: ${roomId}`);
      }
      rooms[roomId].master = socket.id;
      rooms[roomId].pendingMasterSocketId = null;

      if (rooms[roomId].isStreaming) {
        io.to(roomId).emit('stream-started');
        io.to(roomId).emit('streamStarted');
      }
    } else {
      const existing = rooms[roomId].clients.findIndex(c => c.socketId === socket.id);
      if (existing === -1) {
        rooms[roomId].clients.push({
          socketId: socket.id,
          deviceId: deviceId,
          role: 'CLIENT',
          connectedAt: timeString
        });
      }
    }

    const assignedSlot = assignSlot(roomId, socket.id);
    if (assignedSlot !== null) {
      socket.emit('assign-slot', { deviceId: deviceId, slot: assignedSlot });
    }

    if (rooms[roomId].isStreaming) {
      socket.emit('stream-started');
      socket.emit('streamStarted');
    }

    if (rooms[roomId].streamWidth && rooms[roomId].streamHeight) {
      socket.emit('stream-resolution', {
        width: rooms[roomId].streamWidth,
        height: rooms[roomId].streamHeight
      });
    }

    // 🟢 YENİ: Odaya sonradan katılan client, mevcut ses konfigürasyonunu öğrenir
    if (rooms[roomId].soundConfig) {
      socket.emit('sound-config', rooms[roomId].soundConfig);
    }

    io.to(roomId).emit('room:update', {
      count: rooms[roomId].clients.length,
      clients: rooms[roomId].clients
    });

    socket.to(roomId).emit('user-joined', {
      socketId: socket.id,
      isMaster: isMaster,
      connectedAt: timeString
    });
  });

  socket.on('get-room-list', (roomId) => {
    if (rooms[roomId]) {
      io.to(roomId).emit('room:update', {
        count: rooms[roomId].clients.length,
        clients: rooms[roomId].clients
      });
    }
  });

  socket.on('start-stream', (data) => {
    const roomId = data.roomId;
    if (rooms[roomId] && rooms[roomId].master === socket.id) {
      rooms[roomId].isStreaming = true;
      io.to(roomId).emit('stream-started');
      io.to(roomId).emit('streamStarted');
      console.log(`[SERVER] Stream started: ${roomId}`);
    }
  });

  socket.on('stop-stream', (data) => {
    const roomId = data.roomId;
    if (rooms[roomId] && rooms[roomId].master === socket.id) {
      rooms[roomId].isStreaming = false;
      io.to(roomId).emit('stream-stopped');
      io.to(roomId).emit('streamStopped');
    }
  });

  socket.on('stream-resolution', (data) => {
    const roomId = data.roomId;
    if (rooms[roomId] && rooms[roomId].master === socket.id) {
      rooms[roomId].streamWidth = data.width;
      rooms[roomId].streamHeight = data.height;
      socket.to(roomId).emit('stream-resolution', {
        width: data.width,
        height: data.height
      });
      console.log(`[SERVER] Stream resolution: ${roomId} => ${data.width}x${data.height}`);
    }
  });

  // 🟢 YENİ: Ses modu + cihaz başına pan değerlerini sakla ve odaya yayınla
  // data: { roomId, mode: 'standard' | 'surround3D', pans: { deviceId: panValue } }
  socket.on('sound-config', (data) => {
    const roomId = data.roomId;
    if (rooms[roomId] && rooms[roomId].master === socket.id) {
      rooms[roomId].soundConfig = {
        mode: data.mode,
        pans: data.pans || {}
      };
      socket.to(roomId).emit('sound-config', rooms[roomId].soundConfig);
      console.log(`[SERVER] Sound config: ${roomId} => mode=${data.mode}`);
    }
  });

  socket.on('frame_data', (data) => {
    const roomId = socket.roomId;
    if (!rooms[roomId] || rooms[roomId].master !== socket.id || !rooms[roomId].isStreaming) return;

    if (Buffer.isBuffer(data)) {
      socket.to(roomId).emit('frame_data', data);
      return;
    }

    if (data && typeof data === 'object' && data.targetDeviceId) {
      const targetClient = rooms[roomId].clients.find(c =>
        c.role === 'CLIENT' && c.deviceId === data.targetDeviceId
      );
      if (targetClient) {
        io.to(targetClient.socketId).emit('frame_data', data);
        return;
      }
    }

    socket.to(roomId).emit('frame_data', data);
  });

  socket.on('audio_data', (data) => {
    const roomId = socket.roomId;
    if (rooms[roomId] && rooms[roomId].master === socket.id && rooms[roomId].isStreaming) {
      socket.to(roomId).emit('audio_data', data);
    }
  });

  socket.on('disconnect', () => {
    const roomId = socket.roomId;
    if (!roomId || !rooms[roomId]) return;

    if (socket.isMaster) {
      console.log(`[SERVER] Master disconnected, grace period starting (${MASTER_GRACE_MS}ms): ${roomId}`);
      rooms[roomId].pendingMasterSocketId = socket.id;

      disconnectTimers[roomId] = setTimeout(() => {
        if (rooms[roomId] && rooms[roomId].pendingMasterSocketId === socket.id) {
          console.log(`[SERVER] Grace period expired, deleting room: ${roomId}`);
          delete rooms[roomId];
          io.to(roomId).emit('master-left');
        }
        delete disconnectTimers[roomId];
      }, MASTER_GRACE_MS);
    } else {
      rooms[roomId].clients = rooms[roomId].clients.filter(c => c.socketId !== socket.id);
      delete rooms[roomId].slotAssignments[socket.id];
      io.to(roomId).emit('room:update', {
        count: rooms[roomId].clients.length,
        clients: rooms[roomId].clients
      });
    }
  });
});

function assignSlot(roomId, socketId) {
  if (!rooms[roomId]) return null;
  const usedSlots = Object.values(rooms[roomId].slotAssignments);
  for (let i = 1; i <= 9; i++) {
    if (!usedSlots.includes(i)) {
      rooms[roomId].slotAssignments[socketId] = i;
      return i;
    }
  }
  return null;
}

app.get('/', (req, res) => res.send('Screen Relay Server Active!'));

server.listen(PORT, () => console.log(`[SERVER] Running on port ${PORT}`));
