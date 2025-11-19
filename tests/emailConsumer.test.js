// ============================================================
// TESTS UNITARIOS: EMAIL CONSUMER
// ============================================================

const { Kafka } = require('kafkajs');
const EmailConsumer = require('../src/consumer/emailConsumer');
const emailService = require('../src/services/emailService');

// Mock de KafkaJS
jest.mock('kafkajs');

// Mock de emailService
jest.mock('../src/services/emailService');

describe('EmailConsumer - Tests Unitarios', () => {
    let emailConsumer;
    let mockConsumer;
    let mockKafka;

    beforeEach(() => {
        // Limpiar mocks antes de cada test
        jest.clearAllMocks();

        // Configurar mock del consumer de Kafka
        mockConsumer = {
            connect: jest.fn().mockResolvedValue(undefined),
            disconnect: jest.fn().mockResolvedValue(undefined),
            subscribe: jest.fn().mockResolvedValue(undefined),
            run: jest.fn().mockResolvedValue(undefined),
            commitOffsets: jest.fn().mockResolvedValue(undefined)
        };

        // Configurar mock de Kafka
        mockKafka = {
            consumer: jest.fn().mockReturnValue(mockConsumer)
        };

        // Mock del constructor de Kafka
        Kafka.mockImplementation(() => mockKafka);

        // Crear instancia del consumer
        emailConsumer = new EmailConsumer();
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    // ============================================================
    // TEST 1: Verificar conexión a Kafka
    // ============================================================
    describe('Test 1 - Conexión a Kafka', () => {
        test('Debe conectarse correctamente al broker de Kafka', async () => {
            // Ejecutar
            const result = await emailConsumer.connect();

            // Verificar
            expect(result).toBe(true);
            expect(mockConsumer.connect).toHaveBeenCalledTimes(1);
            expect(Kafka).toHaveBeenCalledWith({
                clientId: 'email-consumer',
                brokers: ['localhost:9092']
            });
        });

        test('Debe lanzar error si falla la conexión a Kafka', async () => {
            // Configurar mock para que falle
            mockConsumer.connect.mockRejectedValueOnce(new Error('Connection failed'));

            // Ejecutar y verificar
            await expect(emailConsumer.connect()).rejects.toThrow('Connection failed');
            expect(mockConsumer.connect).toHaveBeenCalledTimes(1);
        });
    });

    // ============================================================
    // TEST 2: Verificar suscripción al topic
    // ============================================================
    describe('Test 2 - Suscripción al topic', () => {
        test('Debe suscribirse al topic "email.notifications" con fromBeginning: true', async () => {
            // Ejecutar
            await emailConsumer.subscribe();

            // Verificar
            expect(mockConsumer.subscribe).toHaveBeenCalledTimes(1);
            expect(mockConsumer.subscribe).toHaveBeenCalledWith({
                topic: 'email.notifications',
                fromBeginning: true
            });
        });

        test('Debe lanzar error si falla la suscripción', async () => {
            // Configurar mock para que falle
            mockConsumer.subscribe.mockRejectedValueOnce(new Error('Subscription failed'));

            // Ejecutar y verificar
            await expect(emailConsumer.subscribe()).rejects.toThrow('Subscription failed');
        });
    });

    // ============================================================
    // TEST 3: Procesar mensaje válido de tipo REPORT_GENERATED
    // ============================================================
    describe('Test 3 - Procesamiento de mensaje válido', () => {
        test('Debe procesar correctamente un evento REPORT_GENERATED', async () => {
            // Preparar
            const mockEventData = {
                traceId: 'test-trace-123',
                eventType: 'REPORT_GENERATED',
                username: 'test_user',
                timestamp: '2025-11-19T10:00:00.000Z',
                action: 'Generación de Reporte de Estadísticas',
                reportData: {
                    title: 'Reporte de Prueba',
                    description: 'Descripción del reporte'
                }
            };

            const mockMessage = {
                offset: '42',
                value: Buffer.from(JSON.stringify(mockEventData))
            };

            // Mock del emailService
            emailService.sendReportGeneratedEmail.mockResolvedValueOnce({
                messageId: 'email-123'
            });

            // Ejecutar
            await emailConsumer.processMessage('email.notifications', 0, mockMessage);

            // Verificar
            expect(emailService.sendReportGeneratedEmail).toHaveBeenCalledTimes(1);
            expect(emailService.sendReportGeneratedEmail).toHaveBeenCalledWith(mockEventData);
        });

        test('Debe procesar correctamente un evento con eventType "report.generated"', async () => {
            // Preparar con formato alternativo del eventType
            const mockEventData = {
                traceId: 'test-trace-456',
                eventType: 'report.generated',
                username: 'admin_user',
                timestamp: '2025-11-19T11:00:00.000Z',
                action: 'Reporte generado',
                reportData: {}
            };

            const mockMessage = {
                offset: '43',
                value: Buffer.from(JSON.stringify(mockEventData))
            };

            emailService.sendReportGeneratedEmail.mockResolvedValueOnce({
                messageId: 'email-456'
            });

            // Ejecutar
            await emailConsumer.processMessage('email.notifications', 0, mockMessage);

            // Verificar
            expect(emailService.sendReportGeneratedEmail).toHaveBeenCalledTimes(1);
        });
    });

    // ============================================================
    // TEST 4: Rechazar mensaje con formato inválido
    // ============================================================
    describe('Test 4 - Manejo de mensajes con formato inválido', () => {
        test('Debe manejar mensaje con JSON malformado sin lanzar error', async () => {
            // Preparar mensaje con JSON inválido
            const mockMessage = {
                offset: '44',
                value: Buffer.from('{ invalid json }')
            };

            // Spy en console.error para verificar logging
            const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation();

            // Ejecutar y verificar que lanza error
            await expect(emailConsumer.processMessage('email.notifications', 0, mockMessage))
                .rejects.toThrow();

            // Verificar
            expect(consoleErrorSpy).toHaveBeenCalled();
            expect(emailService.sendReportGeneratedEmail).not.toHaveBeenCalled();

            consoleErrorSpy.mockRestore();
        });

        test('Debe ignorar eventos de tipo desconocido', async () => {
            // Preparar evento con tipo no manejado
            const mockEventData = {
                traceId: 'test-trace-789',
                eventType: 'UNKNOWN_EVENT_TYPE',
                username: 'test_user'
            };

            const mockMessage = {
                offset: '45',
                value: Buffer.from(JSON.stringify(mockEventData))
            };

            // Spy en console.log para verificar warning
            const consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();

            // Ejecutar
            await emailConsumer.processMessage('email.notifications', 0, mockMessage);

            // Verificar
            expect(emailService.sendReportGeneratedEmail).not.toHaveBeenCalled();
            expect(consoleLogSpy).toHaveBeenCalledWith(
                expect.stringContaining('Tipo de evento no manejado')
            );

            consoleLogSpy.mockRestore();
        });
    });

    // ============================================================
    // TEST 5: Verificar commit y resolveOffset después de procesamiento
    // ============================================================
    describe('Test 5 - Commit y resolveOffset', () => {
        test('Debe hacer commit del offset después de procesar exitosamente', async () => {
            // Preparar
            const mockEventData = {
                traceId: 'test-trace-commit',
                eventType: 'REPORT_GENERATED',
                username: 'test_user',
                timestamp: '2025-11-19T12:00:00.000Z',
                action: 'Test commit',
                reportData: {}
            };

            const mockMessage = {
                offset: '50',
                value: Buffer.from(JSON.stringify(mockEventData))
            };

            const mockResolveOffset = jest.fn();
            const mockBatch = {
                topic: 'email.notifications',
                partition: 0,
                messages: [mockMessage],
                highWatermark: '51',
                firstOffset: jest.fn().mockReturnValue('50'),
                lastOffset: jest.fn().mockReturnValue('50')
            };
            
            const mockUncommittedOffsets = jest.fn().mockReturnValue({});
            const mockHeartbeat = jest.fn();

            emailService.sendReportGeneratedEmail.mockResolvedValueOnce({
                messageId: 'email-commit-test'
            });

            // Simular el procesamiento dentro de eachBatch
            mockConsumer.run.mockImplementationOnce(async ({ eachBatch }) => {
                await eachBatch({
                    batch: mockBatch,
                    resolveOffset: mockResolveOffset,
                    heartbeat: mockHeartbeat,
                    uncommittedOffsets: mockUncommittedOffsets,
                    isRunning: () => true,
                    isStale: () => false
                });
            });

            // Ejecutar
            await emailConsumer.startConsuming();

            // Dar tiempo para que se procese el mensaje
            await new Promise(resolve => setTimeout(resolve, 100));

            // Verificar
            expect(emailService.sendReportGeneratedEmail).toHaveBeenCalledTimes(1);
            expect(mockConsumer.commitOffsets).toHaveBeenCalledTimes(1);
            expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([{
                topic: 'email.notifications',
                partition: 0,
                offset: '51' // offset + 1
            }]);
            expect(mockResolveOffset).toHaveBeenCalledTimes(1);
            expect(mockResolveOffset).toHaveBeenCalledWith('50');
        });

        test('NO debe hacer commit si el procesamiento falla', async () => {
            // Preparar
            const mockEventData = {
                traceId: 'test-trace-fail',
                eventType: 'REPORT_GENERATED',
                username: 'test_user',
                timestamp: '2025-11-19T12:00:00.000Z',
                action: 'Test fail',
                reportData: {}
            };

            const mockMessage = {
                offset: '60',
                value: Buffer.from(JSON.stringify(mockEventData))
            };

            const mockResolveOffset = jest.fn();
            const mockBatch = {
                topic: 'email.notifications',
                partition: 0,
                messages: [mockMessage],
                highWatermark: '61',
                firstOffset: jest.fn().mockReturnValue('60'),
                lastOffset: jest.fn().mockReturnValue('60')
            };
            
            const mockUncommittedOffsets = jest.fn().mockReturnValue({});
            const mockHeartbeat = jest.fn();

            // Mock del emailService para que falle
            emailService.sendReportGeneratedEmail.mockRejectedValueOnce(
                new Error('Email sending failed')
            );

            // Spy en console.error
            const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation();

            // Simular el procesamiento dentro de eachBatch
            mockConsumer.run.mockImplementationOnce(async ({ eachBatch }) => {
                await eachBatch({
                    batch: mockBatch,
                    resolveOffset: mockResolveOffset,
                    heartbeat: mockHeartbeat,
                    uncommittedOffsets: mockUncommittedOffsets,
                    isRunning: () => true,
                    isStale: () => false
                });
            });

            // Ejecutar
            await emailConsumer.startConsuming();

            // Dar tiempo para que se procese el mensaje
            await new Promise(resolve => setTimeout(resolve, 100));

            // Verificar
            expect(emailService.sendReportGeneratedEmail).toHaveBeenCalledTimes(1);
            expect(mockConsumer.commitOffsets).not.toHaveBeenCalled();
            expect(mockResolveOffset).not.toHaveBeenCalled();
            expect(consoleErrorSpy).toHaveBeenCalled();

            consoleErrorSpy.mockRestore();
        });
    });
});
