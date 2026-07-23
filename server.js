const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// Socket.IO ayarları - telefondan bağlanabilsin diye CORS açık
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const PORT = process.env.PORT || 10000;

// Oda sistemi: her "yayın" bir oda olacak
const rooms = {};

io.on('connection', (socket) => {
  console.log('Bir cihaz baglandi:', socket.id);

  // Telefon odaya katılır - ARTIK OBJE GELIYOR
  socket.on('join-room', (data) => {
    const roomId = data.roomId;
    const isMaster = data.isMaster;
    const deviceId = data.deviceId;

    socket.join(roomId);
    socket.roomId = roomId;
    socket.isMaster = isMaster;

    if (!rooms[roomId]) {
      rooms[roomId] = { master: null, clients: [] };
    }

    if (isMaster) {
      rooms[roomId].master = socket.id;
      console.log(`Master odaya katildi: ${roomId}`);
    } else {
      rooms[roomId].clients.push(socket.id);
      console.log(`Katılımcı odaya katildi: ${roomId} - ${deviceId}`);
    }

    // Oda sayısını herkese yolla
    io.to(roomId).emit('room:update', { count: rooms[roomId].clients.length + (rooms[roomId].master? 1 : 0) });
    // Yeni biri geldi diye haber ver
    socket.to(roomId).emit('user-joined', { socketId: socket.id, isMaster: isMaster });
  });

  // Master'dan gelen ekran verisini diğerlerine yayınla
  socket.on('screen-data', (roomId, data) => {
    if (rooms[roomId] && rooms[roomId].master === socket.id) {
      socket.to(roomId).emit('screen-data', data);
    }
  });

  // Master'dan gelen ses verisini diğerlerine yayınla
  socket.on('audio-data', (roomId, data, audioType) => {
    if (rooms[roomId] && rooms[roomId].master === socket.id) {
      socket.to(roomId).emit('audio-data', data, audioType);
    }
  });

  // Bağlantı koptu
  socket.on('disconnect', () => {
    console.log('Bir cihaz ayrildi:', socket.id);
    const roomId = socket.roomId;
    if (roomId && rooms[roomId]) {
      if (socket.isMaster) {
        rooms[roomId].master = null;
        io.to(roomId).emit('master-left');
      } else {
        rooms[roomId].clients = rooms[roomId].clients.filter(id => id!== socket.id);
      }
      io.to(roomId).emit('room:update', { count: rooms[roomId].clients.length + (rooms[roomId].master? 1 : 0) });
    }
  });
});

// Health check için
app.get('/', (req, res) => {
  res.send('Ekran Relay Sunucusu Aktif!');
});

server.listen(PORT, () => {
  console.log(`WebSocket Relay Sunucusu ${PORT} portunda ayakta!`);
});
