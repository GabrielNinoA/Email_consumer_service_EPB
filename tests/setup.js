// ============================================================
// SETUP GLOBAL PARA TESTS DEL EMAIL CONSUMER
// ============================================================
// Este archivo configura el entorno de pruebas y mockea dependencias externas

// Configurar variables de entorno para tests
process.env.NODE_ENV = 'test';
process.env.KAFKA_BROKER = 'localhost:9092';
process.env.MESSAGE_PROCESSING_INTERVAL = '2000';
process.env.IDLE_CHECK_INTERVAL = '5000';
process.env.EMAIL_HOST = 'smtp.gmail.com';
process.env.EMAIL_PORT = '587';
process.env.EMAIL_SECURE = 'false';
process.env.EMAIL_USER = 'test@test.com';
process.env.EMAIL_PASSWORD = 'test_password';

// Silenciar console.log en tests para output más limpio
// Descomentar estas líneas si quieres ver los logs durante las pruebas
// global.console.log = jest.fn();
// global.console.error = jest.fn();
// global.console.warn = jest.fn();
