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

  // YENİ: MASTER ODA AÇSIN
  socket.on('create-room', (roomId) => {
    socket.join(roomId);
    socket.roomId = roomId;
    socket.isMaster = true;

    // Odayı master ile başlat
    rooms[roomId] = {
      master: socket.id,
      clients: [
        {
          socketId: socket.id,
          deviceId: 'MASTER',
          connectedAt: new Date().toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
        }
      ]
    };

    console.log(`Oda olusturuldu: ${roomId} - Master: ${socket.id}`);

    // Master a kendi listesini hemen gönder
    io.to(roomId).emit('room:update', {
      count: rooms[roomId].clients.length + 1,
      clients: rooms[roomId].clients
    });
  });

  // Telefon veya Master odaya katılır
  socket.on('join-room', (data) => {
    const roomId = data.roomId;
    const isMaster = data.isMaster;
    const deviceId = data.deviceId || socket.id;

    if (!rooms[roomId]) return; // Oda yoksa çık

    socket.join(roomId);
    socket.roomId = roomId;
    socket.isMaster = isMaster;

    const now = new Date();
    const timeString = now.toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

    if (isMaster) {
      rooms[roomId].master = socket.id;
      console.log(`Master odaya katildi: ${roomId}`);
    } else {
      // Client ise listeye ekle
      const existingClientIndex = rooms[roomId].clients.findIndex(c => c.socketId === socket.id);
      if (existingClientIndex === -1) {
        rooms[roomId].clients.push({
          socketId: socket.id,
          deviceId: deviceId,
          connectedAt: timeString
        });
      }
      console.log(`Katılımcı odaya katildi: ${roomId} - Cihaz: ${deviceId} - Saat: ${timeString}`);
    }

    // Oda güncellendiğinde hem toplam sayıyı hem de bağlı cihazların detaylı listesini gönderiyoruz
    io.to(roomId).emit('room:update', {
      count: rooms[roomId].clients.length + (rooms[roomId].master? 1 : 0),
      clients: rooms[roomId].clients
    });

    // Yeni biri katıldı sinyali
    socket.to(roomId).emit('user-joined', { socketId: socket.id, isMaster: isMaster, connectedAt: timeString });
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
        delete rooms[roomId]; // Master çıkınca odayı komple sil
        io.to(roomId).emit('master-left');
        console.log(`Oda kapatildi: ${roomId}`);
      } else {
        rooms[roomId].clients = rooms[roomId].clients.filter(c => c.socketId!== socket.id);
      }

      // Güncel listeyi odadaki herkese bildir
      if(rooms[roomId]) {
        io.to(roomId).emit('room:update', {
          count: rooms[roomId].clients.length + (rooms[roomId].master? 1 : 0),
          clients: rooms[roomId].clients
        });
      }
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
