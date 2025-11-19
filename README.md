# Email Consumer Service

Microservicio consumidor de eventos de Kafka para el envío de notificaciones por email.

## Descripción

Este servicio es parte de la arquitectura de eventos del Sistema de Quejas Boyacá. Su función es:
- Consumir eventos del tópico `email.notifications` de Kafka
- Procesar notificaciones de reportes generados
- Enviar emails a través de Gmail SMTP
- Mantener trazabilidad con trace IDs
- Procesar mensajes en lotes de 10

## Configuración

### Variables de Entorno (.env)

```env
# Kafka Configuration
KAFKA_BROKER=localhost:9092
KAFKA_CLIENT_ID=email-consumer-service
KAFKA_GROUP_ID=email-consumer-group

# Email Configuration (Gmail SMTP)
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_SECURE=false
EMAIL_USER=quejasboyaca746@gmail.com
EMAIL_PASS=tu_password_de_aplicacion
EMAIL_FROM=Sistema de Quejas Boyacá <quejasboyaca746@gmail.com>
EMAIL_NOTIFICATION_TO=quejasboyaca746@gmail.com

# Service Configuration
NODE_ENV=development
SERVICE_PORT=3003
```

### Dependencias

```json
{
  "kafkajs": "^2.2.4",
  "nodemailer": "^6.9.0",
  "dotenv": "^16.3.1"
}
```

## Instalación

```bash
cd Email_Consumer_Service
npm install
```

## Ejecución

### Producción
```bash
npm start
```

### Desarrollo (con nodemon)
```bash
npm run dev
```

## Estructura del Proyecto

```
Email_Consumer_Service/
├── src/
│   ├── consumer/
│   │   └── emailConsumer.js    # Consumidor de Kafka
│   └── services/
│       └── emailService.js      # Servicio de envío de emails
├── server.js                     # Punto de entrada
├── package.json
├── .env                          # Configuración
└── README.md
```

## Formato de Eventos

### Evento de Entrada (Kafka)

```json
{
  "traceId": "uuid-v4-trace-id",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "username": "admin_user",
  "eventData": {
    "type": "REPORT_GENERATED",
    "reportData": {
      "tipo": "Reportes Generales",
      "totalRegistros": 150,
      "estadisticas": {
        "total_quejas": 150,
        "total_entidades": 25,
        "quejas_hoy": 5,
        "quejas_mes_actual": 45
      },
      "responseTime": 234
    },
    "timestamp": "2024-01-15T10:30:00.000Z"
  }
}
```

## Procesamiento

1. **Consumo por Lotes**: Procesa hasta 10 mensajes por lote
2. **Validación**: Verifica la estructura del evento
3. **Envío de Email**: Usa Nodemailer con Gmail SMTP
4. **Commit Manual**: Confirma solo mensajes procesados exitosamente
5. **Logging**: Registra trace IDs en cada operación

## Email Template

### Asunto
`📊 Nuevo Reporte Generado - Sistema de Quejas Boyacá`

### Contenido
- Usuario que generó el reporte
- Fecha y hora de generación
- Acción realizada (REPORT_GENERATED)
- Estadísticas del reporte
- Trace ID para seguimiento

## Logs

```
📧 Email Consumer Service iniciado
📩 Conectado a Kafka broker: localhost:9092
📬 Consumiendo del tópico: email.notifications
✅ Procesando lote de 3 mensajes
📧 Enviando email para evento: uuid-trace-id
✅ Email enviado exitosamente: <message-id>
✅ Lote de 3 mensajes procesado y confirmado
```

## Manejo de Errores

- **Kafka no disponible**: Reintenta conexión automáticamente
- **Error en email individual**: Registra error pero continúa con el lote
- **Gmail SMTP error**: Registra error y mantiene el mensaje para reintento
- **Evento inválido**: Registra advertencia y continúa

## Persistencia de Cola

Kafka mantiene los mensajes persistidos incluso cuando el consumidor está offline. Los mensajes se acumulan y se procesan cuando el servicio vuelve a estar disponible.

## Monitoreo

### Health Check
El servicio no expone endpoints HTTP por defecto, pero se puede verificar:
- Logs de consola
- Estado de conexión a Kafka
- Mensajes procesados exitosamente

## Seguridad

⚠️ **IMPORTANTE**: 
- Usa contraseñas de aplicación de Gmail (no contraseña de cuenta)
- No commits archivos `.env` al repositorio
- Considera usar variables de entorno del sistema en producción

## Troubleshooting

### Kafka no se conecta
```bash
# Verificar que Kafka está corriendo
docker ps | grep kafka

# Ver logs de Kafka
cd ../kafka-broker
docker-compose logs -f kafka
```

### Gmail rechaza emails
- Verificar contraseña de aplicación
- Activar "Acceso de apps menos seguras" si es necesario
- Verificar límites de envío diarios de Gmail

### Mensajes no se consumen
- Verificar que el tópico `email.notifications` existe
- Verificar el GROUP_ID del consumidor
- Revisar logs de Kafka para errores

## Integración con Sistema Principal

El backend principal publica eventos usando `src/events/eventProducer.js`:

```javascript
const eventProducer = require('../events/eventProducer');

// En estadisticasController.js
await eventProducer.publishEmailEvent(eventData, username);
```

## Futuras Mejoras

- [ ] Health check endpoint HTTP
- [ ] Métricas de procesamiento (Prometheus)
- [ ] Retry con backoff exponencial
- [ ] Dead Letter Queue para mensajes fallidos
- [ ] Templates HTML más sofisticados
- [ ] Soporte para múltiples proveedores de email
