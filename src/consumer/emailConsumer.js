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
            autoCommit: false
        });

        this.isRunning = false;
        this.consumerId = `email-consumer-${Date.now()}`;
        
        // Configuración de intervalos de consumo
        this.MESSAGE_PROCESSING_INTERVAL = parseInt(process.env.MESSAGE_PROCESSING_INTERVAL) || 2000; // 2 segundos entre mensajes
        this.IDLE_CHECK_INTERVAL = parseInt(process.env.IDLE_CHECK_INTERVAL) || 5000; // 5 segundos cuando no hay mensajes
        this.pendingMessages = [];
        this.isProcessing = false;
        this.processingInterval = null;
        
        console.log(`[CONSUMER] Configuración de intervalos:`);
        console.log(`[CONSUMER] Procesamiento de mensajes: cada ${this.MESSAGE_PROCESSING_INTERVAL}ms`);
        console.log(`[CONSUMER] Revisión cuando no hay mensajes: cada ${this.IDLE_CHECK_INTERVAL}ms`);
    }

    async connect() {
        try {
            await this.consumer.connect();
            console.log(`[CONSUMER] Conectado a Kafka [ConsumerID: ${this.consumerId}]`);
            return true;
        } catch (error) {
            console.error('[CONSUMER] Error conectando a Kafka:', error.message);
            throw error;
        }
    }

    async subscribe() {
        try {
            await this.consumer.subscribe({ 
                topic: 'email.notifications',
                fromBeginning: true
            });
            console.log('[CONSUMER] Suscrito al topic: email.notifications (con persistencia)');
        } catch (error) {
            console.error('[CONSUMER] Error suscribiendo al topic:', error.message);
            throw error;
        }
    }

    async processMessage(topic, partition, message) {
        const startTime = Date.now();
        let eventData = null;
        let traceId = 'unknown';

        try {
            eventData = JSON.parse(message.value.toString());
            traceId = eventData.traceId || 'unknown';

            console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
            console.log(`[CONSUMER] Procesando mensaje`);
            console.log(`[TraceID: ${traceId}]`);
            console.log(`[Topic: ${topic}] [Partition: ${partition}] [Offset: ${message.offset}]`);
            console.log(`[ConsumerID: ${this.consumerId}]`);
            console.log(`[Timestamp: ${new Date().toISOString()}]`);
            console.log(`[Event Type: ${eventData.eventType}]`);
            console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

            // Validar tipo de evento
            if (eventData.eventType !== 'REPORT_GENERATED' && eventData.eventType !== 'report.generated') {
                console.log(`[CONSUMER] Tipo de evento no manejado: ${eventData.eventType} [TraceID: ${traceId}]`);
                // No hacer commit aquí - se maneja en startConsuming para eventos no manejados
                return; // Retorna para que startConsuming haga el commit
            }

            // Enviar email
            console.log(`[CONSUMER] Enviando email [TraceID: ${traceId}]`);
            const emailResult = await emailService.sendReportGeneratedEmail(eventData);
            
            const processingTime = Date.now() - startTime;
            
            console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
            console.log(`[CONSUMER] Email enviado exitosamente`);
            console.log(`[TraceID: ${traceId}]`);
            console.log(`[MessageID: ${emailResult.messageId}]`);
            console.log(`[Processing Time: ${processingTime}ms]`);
            console.log(`[Timestamp: ${new Date().toISOString()}]`);
            console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
            console.log(`[CONSUMER] Mensaje procesado - offset ${message.offset} será confirmado`);
            console.log(`[CONSUMER] Esperando ${this.MESSAGE_PROCESSING_INTERVAL}ms antes del siguiente mensaje...\n`);

            // ❌ NO hacer commit aquí - se maneja en startConsuming después de procesar

        } catch (error) {
            const processingTime = Date.now() - startTime;
            
            console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
            console.error(`[CONSUMER] Error procesando mensaje`);
            console.error(`[TraceID: ${traceId}]`);
            console.error(`[Error: ${error.message}]`);
            console.error(`[Processing Time: ${processingTime}ms]`);
            console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
            console.error(`[CONSUMER] Error - offset ${message.offset} NO será confirmado\n`);
            
            // Re-lanzar error para que startConsuming NO haga commit de este mensaje
            throw error;
        }
    }

    async startConsuming() {
        this.isRunning = true;
        let hasMessages = false;
        let batchCount = 0;
        
        // Usar eachBatch para tener control total sobre el procesamiento
        await this.consumer.run({
            // Configuración de procesamiento por lotes
            eachBatchAutoResolve: false, // Desactivar auto-resolve para tener control manual
            partitionsConsumedConcurrently: 1, // Procesar una partición a la vez
            
            eachBatch: async ({ batch, resolveOffset, heartbeat, uncommittedOffsets }) => {
                const { topic, partition } = batch;
                batchCount++;
                
                console.log(`[CONSUMER] Batch #${batchCount} recibido:`);
                console.log(`[CONSUMER] Topic: ${topic}, Partition: ${partition}`);
                console.log(`[CONSUMER] Mensajes en batch: ${batch.messages.length}`);
                console.log(`[CONSUMER] Offset inicial: ${batch.firstOffset()}, Offset final: ${batch.lastOffset()}`);
                console.log(`[CONSUMER] highWatermark: ${batch.highWatermark}`);
                console.log(`[CONSUMER] Uncommitted offsets: ${JSON.stringify(uncommittedOffsets())}\n`);
                
                for (let message of batch.messages) {
                    console.log(`[CONSUMER] Procesando mensaje ${message.offset} de ${batch.messages.length} mensajes en el batch\n`);
                    
                    if (!this.isRunning) {
                        console.log(`[CONSUMER] Consumer detenido, saliendo del loop\n`);
                        break;
                    }

                    hasMessages = true;

                    try {
                        // Procesar mensaje
                        await this.processMessage(topic, partition, message);

                        // COMMIT INMEDIATO después de cada mensaje exitoso
                        const offsetToCommit = (parseInt(message.offset) + 1).toString();
                        console.log(`[CONSUMER] Haciendo commit del offset ${message.offset} (next: ${offsetToCommit})...`);
                        
                        await this.consumer.commitOffsets([
                            {
                                topic,
                                partition,
                                offset: offsetToCommit
                            }
                        ]);
                        
                        console.log(`[CONSUMER] Offset ${message.offset} confirmado y eliminado de cola`);
                        
                        // Marcar offset como resuelto para que Kafka sepa que lo procesamos
                        resolveOffset(message.offset);
                        console.log(`[CONSUMER] Offset ${message.offset} marcado como resuelto en batch\n`);

                    } catch (error) {
                        console.error(`[CONSUMER] Error en mensaje offset ${message.offset}: ${error.message}`);
                        console.error(`[CONSUMER] NO se hará commit, quedará en cola para reintento\n`);
                        // NO hacer commit - el mensaje quedará pendiente para reprocesar
                        // NO llamar resolveOffset para que Kafka sepa que este mensaje NO se procesó
                    }

                    // Esperar el intervalo configurado antes del siguiente mensaje
                    if (this.isRunning && batch.messages.indexOf(message) < batch.messages.length - 1) {
                        console.log(`[CONSUMER] Esperando ${this.MESSAGE_PROCESSING_INTERVAL}ms antes del siguiente mensaje del batch...\n`);
                        await new Promise(resolve => setTimeout(resolve, this.MESSAGE_PROCESSING_INTERVAL));
                    }

                    // Mantener heartbeat con Kafka
                    await heartbeat();
                }
                
                console.log(`[CONSUMER] Batch #${batchCount} completado. Mensajes procesados: ${batch.messages.length}\n`);

                // Si no hubo mensajes, esperar más tiempo antes de revisar nuevamente
                if (!hasMessages) {
                    console.log(`[CONSUMER] No hay mensajes pendientes. Revisando nuevamente en ${this.IDLE_CHECK_INTERVAL}ms...`);
                    await new Promise(resolve => setTimeout(resolve, this.IDLE_CHECK_INTERVAL));
                }
                
                hasMessages = false;
            }
        });
    }

    async disconnect() {
        try {
            this.isRunning = false;
            await this.consumer.disconnect();
            console.log('[CONSUMER] Desconectado de Kafka');
        } catch (error) {
            console.error('[CONSUMER] Error desconectando:', error.message);
        }
    }

    async start() {
        try {
            await this.connect();
            await this.subscribe();
            await this.startConsuming();
        } catch (error) {
            console.error('[CONSUMER] Error iniciando consumer:', error.message);
            throw error;
        }
    }
}

module.exports = EmailConsumer;
