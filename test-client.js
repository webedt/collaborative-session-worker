const WebSocket = require('ws');

// Connect to the server
const ws = new WebSocket('ws://localhost:8080');

const sessionId = 'test-session-' + Date.now();
const userId = 'test-user-1';

ws.on('open', () => {
  console.log('\n=== Connected to server ===\n');

  // Join a session
  console.log(`Joining session: ${sessionId}`);
  ws.send(JSON.stringify({
    type: 'join',
    sessionId: sessionId,
    userId: userId
  }));
});

ws.on('message', (data) => {
  const message = JSON.parse(data.toString());
  console.log('Received:', JSON.stringify(message, null, 2));

  // After joining, test file operations
  if (message.type === 'joined') {
    console.log('\n=== Successfully joined! Testing file operations... ===\n');

    setTimeout(() => {
      // 1. Create a file
      console.log('Creating file: hello.txt');
      ws.send(JSON.stringify({
        type: 'fileOperation',
        data: {
          type: 'create',
          path: 'hello.txt',
          content: 'Hello, collaborative world!',
          userId: userId,
          timestamp: new Date().toISOString()
        }
      }));
    }, 500);

    setTimeout(() => {
      // 2. Get files list
      console.log('\nGetting files list...');
      ws.send(JSON.stringify({
        type: 'getFiles',
        data: {}
      }));
    }, 1000);

    setTimeout(() => {
      // 3. Read the file
      console.log('\nReading hello.txt...');
      ws.send(JSON.stringify({
        type: 'getFile',
        data: {
          path: 'hello.txt'
        }
      }));
    }, 1500);

    setTimeout(() => {
      // 4. Update the file
      console.log('\nUpdating hello.txt...');
      ws.send(JSON.stringify({
        type: 'fileOperation',
        data: {
          type: 'update',
          path: 'hello.txt',
          content: 'Hello, collaborative world! This is an update.',
          userId: userId,
          timestamp: new Date().toISOString()
        }
      }));
    }, 2000);

    setTimeout(() => {
      // 5. Create another file
      console.log('\nCreating file: test.js');
      ws.send(JSON.stringify({
        type: 'fileOperation',
        data: {
          type: 'create',
          path: 'test.js',
          content: 'console.log("Testing collaborative session");',
          userId: userId,
          timestamp: new Date().toISOString()
        }
      }));
    }, 2500);

    setTimeout(() => {
      // 6. Get updated files list
      console.log('\nGetting updated files list...');
      ws.send(JSON.stringify({
        type: 'getFiles',
        data: {}
      }));
    }, 3000);

    setTimeout(() => {
      console.log('\n=== Test complete! Closing connection... ===\n');
      ws.close();
    }, 4000);
  }
});

ws.on('error', (error) => {
  console.error('WebSocket error:', error);
});

ws.on('close', () => {
  console.log('Connection closed');
  process.exit(0);
});
