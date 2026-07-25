const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  maxHttpBufferSize: 1e8
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
      clients: [
        {
          socketId: socket.id,
          deviceId: 'Master Device',
          role: 'MASTER',
          connectedAt: timeString
        }
      ],
      slotAssignments: {},
      isStreaming: false
    };

    console.log(`[SERVER] Room created: ${roomId} - Master: ${socket.id}`);

    io.to(roomId).emit('room:update', {
      count: rooms[roomId].clients.length,
      clients: rooms[roomId].clients
    });
  });

  socket.on('join-room', (data) => {
    console.log(`[SERVER] join-room received:`, JSON.stringify(data));

    const roomId = data.roomId;
    const isMaster = data.isMaster;
    const deviceId = data.deviceId || socket.id;

    if (!rooms[roomId]) {
      console.log(`[SERVER ERROR] Room not found: ${roomId}`);
      return;
    }

    socket.join(roomId);
    socket.roomId = roomId;
    socket.isMaster = isMaster;

    const timeString = new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

    if (isMaster === true) {
      rooms[roomId].master = socket.id;
      console.log(`[SERVER] Master joined room: ${roomId}`);
    } else {
      const existingClientIndex = rooms[roomId].clients.findIndex(c => c.socketId === socket.id);
      if (existingClientIndex === -1) {
        rooms[roomId].clients.push({
          socketId: socket.id,
          deviceId: deviceId,
          role: 'CLIENT',
          connectedAt: timeString
        });
        console.log(`[SERVER] Client added: ${deviceId} - Room: ${roomId}`);
      }
    }

    const assignedSlot = assignSlot(roomId, socket.id);
    if (assignedSlot !== null) {
      socket.emit('assign-slot', {
        deviceId: deviceId,
        slot: assignedSlot
      });
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
      console.log(`[SERVER] List requested by: ${socket.id} for room: ${roomId}`);
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
      console.log(`[SERVER] Stream started in room: ${roomId}`);
    }
  });

  socket.on('stop-stream', (data) => {
    const roomId = data.roomId;
    if (rooms[roomId] && rooms[roomId].master === socket.id) {
      rooms[roomId].isStreaming = false;
      io.to(roomId).emit('stream-stopped');
      console.log(`[SERVER] Stream stopped in room: ${roomId}`);
    }
  });

  socket.on('frame_data', (data) => {
    const roomId = socket.roomId;
    if (rooms[roomId] && rooms[roomId].master === socket.id && rooms[roomId].isStreaming) {
      const targetIndex = data.index;
      const targetClient = rooms[roomId].clients.find((c) => {
        return c.role === 'CLIENT' && rooms[roomId].slotAssignments[c.socketId] === targetIndex + 1;
      });
      
      if (targetClient) {
        io.to(targetClient.socketId).emit('frame_data', data);
      } else {
        socket.to(roomId).emit('frame_data', data);
      }
    }
  });

  socket.on('audio_data', (data) => {
    const roomId = socket.roomId;
    if (rooms[roomId] && rooms[roomId].master === socket.id && rooms[roomId].isStreaming) {
      socket.to(roomId).emit('audio_data', data);
    }
  });

  socket.on('disconnect', () => {
    console.log('[SERVER] Device disconnected:', socket.id);
    const roomId = socket.roomId;
    if (roomId && rooms[roomId]) {
      if (socket.isMaster) {
        delete rooms[roomId];
        io.to(roomId).emit('master-left');
        console.log(`[SERVER] Room closed: ${roomId}`);
      } else {
        const before = rooms[roomId].clients.length;
        rooms[roomId].clients = rooms[roomId].clients.filter(c => c.socketId !== socket.id);
        delete rooms[roomId].slotAssignments[socket.id];
        console.log(`[SERVER] Client left. Before: ${before}, After: ${rooms[roomId].clients.length}`);
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

app.get('/', (req, res) => {
  res.send('Screen Relay Server Active!');
});

server.listen(PORT, () => {
  console.log(`[SERVER] WebSocket Relay Server running on port ${PORT}`);
});
