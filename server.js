require('dotenv').config();
const EmailConsumer = require('./src/consumer/emailConsumer');

console.log('╔═══════════════════════════════════════════════╗');
console.log('║   📧 EMAIL CONSUMER SERVICE - Starting...    ║');
console.log('╚═══════════════════════════════════════════════╝\n');

const consumer = new EmailConsumer();

// Manejo de señales para cierre graceful
const gracefulShutdown = async (signal) => {
    console.log(`\n⚠️  ${signal} recibido. Cerrando consumer...`);
    try {
        await consumer.disconnect();
        console.log('✅ Consumer cerrado correctamente');
        process.exit(0);
    } catch (error) {
        console.error('❌ Error durante cierre:', error.message);
        process.exit(1);
    }
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Manejo de errores no capturados
process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
    console.error('❌ Uncaught Exception:', error);
    gracefulShutdown('UNCAUGHT_EXCEPTION');
});

// Iniciar consumer
(async () => {
    try {
        await consumer.start();
        console.log('\n╔═══════════════════════════════════════════════╗');
        console.log('║   ✅ EMAIL CONSUMER SERVICE - Running!       ║');
        console.log('║   📬 Esperando mensajes de Kafka...          ║');
        console.log('╚═══════════════════════════════════════════════╝\n');
    } catch (error) {
        console.error('❌ Error fatal iniciando consumer:', error);
        process.exit(1);
    }
})();
