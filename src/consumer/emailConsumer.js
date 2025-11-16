const { Kafka } = require('kafkajs');
const emailService = require('../services/emailService');

class EmailConsumer {
    constructor() {
        this.kafka = new Kafka({
            clientId: 'email-consumer',
            brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
        });

        this.consumer = this.kafka.consumer({ 
            groupId: 'email-notification-group',
            sessionTimeout: 30000,
            heartbeatInterval: 3000,
            // Commit manual para asegurar que solo se confirmen mensajes procesados exitosamente
            autoCommit: false
        });

        this.isRunning = false;
        this.consumerId = `email-consumer-${Date.now()}`;
    }

    async connect() {
        try {
            await this.consumer.connect();
            console.log(`[CONSUMER] ✅ Conectado a Kafka [ConsumerID: ${this.consumerId}]`);
            return true;
        } catch (error) {
            console.error('[CONSUMER] ❌ Error conectando a Kafka:', error.message);
            throw error;
        }
    }

    async subscribe() {
        try {
            await this.consumer.subscribe({ 
                topic: 'email.notifications',
                fromBeginning: true // Procesar mensajes pendientes desde el último commit
            });
            console.log('[CONSUMER] 📬 Suscrito al topic: email.notifications (con persistencia de cola)');
        } catch (error) {
            console.error('[CONSUMER] ❌ Error suscribiendo al topic:', error.message);
            throw error;
        }
    }

    async startConsuming() {
        this.isRunning = true;
        
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const startTime = Date.now();
                let eventData = null;
                let traceId = 'unknown';

                try {
                    // Parse del mensaje
                    eventData = JSON.parse(message.value.toString());
                    traceId = eventData.traceId || 'unknown';

                    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
                    console.log(`[CONSUMER] 📩 Mensaje recibido`);
                    console.log(`[TraceID: ${traceId}]`);
                    console.log(`[Topic: ${topic}] [Partition: ${partition}] [Offset: ${message.offset}]`);
                    console.log(`[ConsumerID: ${this.consumerId}]`);
                    console.log(`[Timestamp: ${new Date().toISOString()}]`);
                    console.log(`[Event Type: ${eventData.eventType}]`);
                    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

                    // Validar tipo de evento (soportar ambos formatos)
                    if (eventData.eventType !== 'REPORT_GENERATED' && eventData.eventType !== 'report.generated') {
                        console.log(`[CONSUMER] ⚠️  Tipo de evento no manejado: ${eventData.eventType} [TraceID: ${traceId}]`);
                        // Hacer commit para no reprocesar este mensaje
                        await this.consumer.commitOffsets([{
                            topic,
                            partition,
                            offset: (parseInt(message.offset) + 1).toString()
                        }]);
                        return;
                    }

                    // Enviar email
                    console.log(`[CONSUMER] 📧 Procesando envío de email [TraceID: ${traceId}]`);
                    
                    const emailResult = await emailService.sendReportGeneratedEmail(eventData);
                    
                    const processingTime = Date.now() - startTime;
                    
                    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
                    console.log(`[CONSUMER] ✅ Email enviado exitosamente`);
                    console.log(`[TraceID: ${traceId}]`);
                    console.log(`[MessageID: ${emailResult.messageId}]`);
                    console.log(`[Processing Time: ${processingTime}ms]`);
                    console.log(`[Timestamp: ${new Date().toISOString()}]`);
                    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

                    // ✅ COMMIT MANUAL - Solo después de procesar exitosamente
                    await this.consumer.commitOffsets([{
                        topic,
                        partition,
                        offset: (parseInt(message.offset) + 1).toString()
                    }]);
                    console.log(`[CONSUMER] ✅ Offset confirmado: ${message.offset} [TraceID: ${traceId}]\n`);

                } catch (error) {
                    const processingTime = Date.now() - startTime;
                    
                    console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
                    console.error(`[CONSUMER] ❌ Error procesando mensaje`);
                    console.error(`[TraceID: ${traceId}]`);
                    console.error(`[Error: ${error.message}]`);
                    console.error(`[Processing Time: ${processingTime}ms]`);
                    console.error(`[Timestamp: ${new Date().toISOString()}]`);
                    console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
                    console.error(`[CONSUMER] ⚠️  Mensaje NO confirmado - Se reprocesará al reiniciar [TraceID: ${traceId}]\n`);
                    
                    // ❌ NO hacer commit - El mensaje se reprocesará en el siguiente reinicio
                    // Esto asegura que ningún mensaje se pierda si falla el envío del email
                }
            }
        });
    }

    async disconnect() {
        try {
            this.isRunning = false;
            await this.consumer.disconnect();
            console.log('[CONSUMER] 🔌 Desconectado de Kafka');
        } catch (error) {
            console.error('[CONSUMER] ❌ Error desconectando:', error.message);
        }
    }

    async start() {
        try {
            await this.connect();
            await this.subscribe();
            await this.startConsuming();
        } catch (error) {
            console.error('[CONSUMER] ❌ Error iniciando consumer:', error.message);
            throw error;
        }
    }
}

module.exports = EmailConsumer;
