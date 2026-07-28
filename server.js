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

io.on('connection', (socket) => {
  console.log('[SERVER] Device connected:', socket.id);

  socket.on('create-room', (roomId) => {
    socket.join(roomId);
    socket.roomId = roomId;
    socket.isMaster = true;

    const timeString = new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

    rooms[roomId] = {
      master: socket.id,
      clients: [{
        socketId: socket.id,
        deviceId: 'Master Device',
        role: 'MASTER',
        connectedAt: timeString
      }],
      slotAssignments: {},
      isStreaming: false
    };

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
      rooms[roomId].master = socket.id;
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

  // ===================================================================
  // YENI: frame_data - HEM JSON HEM BINARY (JPEG) DESTEKLI
  // ===================================================================
  socket.on('frame_data', (data) => {
    const roomId = socket.roomId;
    if (!rooms[roomId] || rooms[roomId].master !== socket.id || !rooms[roomId].isStreaming) return;

    // BINARY FRAME: Buffer/Uint8Array geldi -> direkt broadcast
    if (Buffer.isBuffer(data)) {
      socket.to(roomId).emit('frame_data', data);
      return;
    }

    // JSON FRAME: Eski format (targetDeviceId ile routing)
    if (data && typeof data === 'object' && data.targetDeviceId) {
      const targetClient = rooms[roomId].clients.find(c => 
        c.role === 'CLIENT' && c.deviceId === data.targetDeviceId
      );
      if (targetClient) {
        io.to(targetClient.socketId).emit('frame_data', data);
        return;
      }
    }

    // Eski format veya broadcast
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
    if (roomId && rooms[roomId]) {
      if (socket.isMaster) {
        delete rooms[roomId];
        io.to(roomId).emit('master-left');
      } else {
        rooms[roomId].clients = rooms[roomId].clients.filter(c => c.socketId !== socket.id);
        delete rooms[roomId].slotAssignments[socket.id];
      }
      if (rooms[roomId]) {
        io.to(roomId).emit('room:update', {
          count: rooms[roomId].clients.length,
          clients: rooms[roomId].clients
        });
      }
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
