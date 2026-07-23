const express = require('express');
const http = require('http');
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*", // APK test için. Yayına alınca domainini yaz
    methods: ["GET", "POST"]
  }
});

const PORT = process.env.PORT || 4040;

io.on('connection', (socket) => {
    console.log('Yeni cihaz bağlandı:', socket.id);

    // Master oda açar
    socket.on('host', (roomId) => {
        socket.join(roomId);
        socket.roomId = roomId;
        socket.role = 'master';
        socket.emit('HOST_READY', { roomId: roomId });
        console.log(`Master oda açtı: ${roomId}`);
    });

    // Client odaya girer
    socket.on('join', ({ roomId, payload }) => {
        const room = io.sockets.adapter.rooms.get(roomId);
        if (room && room.size > 0) {
            socket.join(roomId);
            socket.roomId = roomId;
            socket.role = 'client';
            socket.to(roomId).emit('client_connected', { clientId: socket.id, payload: payload });
            socket.emit('CONNECTED_OK', { roomId: roomId });
            console.log(`Client odaya bağlandı: ${roomId}`);
        } else {
            socket.emit('ROOM_NOT_FOUND');
        }
    });

    // Veri aktarımı: Client -> Master
    socket.on('data', (payload) => {
        socket.to(socket.roomId).emit('data', payload);
    });

    socket.on('disconnect', () => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit('client_disconnected', { clientId: socket.id });
            console.log(`Cihaz ayrıldı: ${socket.roomId}`);
        }
    });
});

server.listen(PORT, () => {
    console.log(`WebSocket Relay Sunucusu ${PORT} portunda ayakta!`);
});