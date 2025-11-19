const nodemailer = require('nodemailer');

class EmailService {
    constructor() {
        this.transporter = null;
        this.isConfigured = false;
        this.initializeTransporter();
    }

    initializeTransporter() {
        try {
            if (!process.env.EMAIL_HOST || !process.env.EMAIL_USER || !process.env.EMAIL_PASSWORD) {
                console.warn('[EMAIL-SERVICE] Configuración de email incompleta');
                return;
            }

            this.transporter = nodemailer.createTransport({
                host: process.env.EMAIL_HOST,
                port: parseInt(process.env.EMAIL_PORT) || 587,
                secure: process.env.EMAIL_SECURE === 'true',
                auth: {
                    user: process.env.EMAIL_USER,
                    pass: process.env.EMAIL_PASSWORD
                },
                connectionTimeout: 10000,
                greetingTimeout: 5000,
                socketTimeout: 15000,
                tls: {
                    rejectUnauthorized: process.env.NODE_ENV === 'production'
                }
            });

            this.isConfigured = true;
            console.log('[EMAIL-SERVICE] Servicio de email configurado');
        } catch (error) {
            console.error('[EMAIL-SERVICE] Error configurando email:', error.message);
            this.isConfigured = false;
        }
    }

    async sendReportGeneratedEmail(eventData) {
        if (!this.isConfigured) {
            throw new Error('Email no configurado');
        }

        const { traceId, username, timestamp, action, reportData } = eventData;

        const formattedDate = new Date(timestamp).toLocaleString('es-CO', {
            timeZone: 'America/Bogota',
            year: 'numeric',
            month: 'long',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });

        const subject = `Reporte Generado - Sistema Quejas Boyacá`;

        const html = `
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
                .container { max-width: 600px; margin: 0 auto; padding: 20px; }
                .header { background-color: #2c5aa0; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }
                .content { background-color: #f9f9f9; padding: 20px; border-radius: 0 0 8px 8px; }
                .info-box { background-color: white; padding: 15px; margin: 10px 0; border-left: 4px solid #2c5aa0; }
                .footer { text-align: center; margin-top: 20px; font-size: 12px; color: #666; }
                .highlight { color: #2c5aa0; font-weight: bold; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Reporte Generado</h1>
                    <p>Sistema de Quejas - Departamento de Boyacá</p>
                </div>
                
                <div class="content">
                    <div class="info-box">
                        <h3>Información del Usuario</h3>
                        <p><strong>Nombre de Usuario:</strong> <span class="highlight">${username || 'Sistema'}</span></p>
                        <p><strong>Hora y Fecha:</strong> ${formattedDate}</p>
                        <p><strong>Acción:</strong> ${action}</p>
                    </div>

                    ${reportData ? `
                    <div class="info-box">
                        <h3>Resumen del Reporte</h3>
                        <p><strong>Total de Quejas:</strong> ${reportData.total_quejas || 'N/A'}</p>
                        <p><strong>Total de Entidades:</strong> ${reportData.total_entidades || 'N/A'}</p>
                        <p><strong>Quejas del Día:</strong> ${reportData.quejas_hoy || 'N/A'}</p>
                        <p><strong>Quejas del Mes:</strong> ${reportData.quejas_mes_actual || 'N/A'}</p>
                    </div>
                    ` : ''}

                    <div class="info-box">
                        <h3>Trazabilidad</h3>
                        <p><strong>Trace ID:</strong> <code>${traceId}</code></p>
                        <p><strong>Timestamp:</strong> ${timestamp}</p>
                    </div>

                    <div class="footer">
                        <p>Este es un mensaje automático del Sistema de Quejas del Departamento de Boyacá.</p>
                        <p>Generado el ${formattedDate}</p>
                    </div>
                </div>
            </div>
        </body>
        </html>
        `;

        const text = `
REPORTE GENERADO - Sistema de Quejas Boyacá

INFORMACIÓN DEL USUARIO:
- Nombre de Usuario: ${username || 'Sistema'}
- Hora y Fecha: ${formattedDate}
- Acción: ${action}

${reportData ? `
RESUMEN DEL REPORTE:
- Total de Quejas: ${reportData.total_quejas || 'N/A'}
- Total de Entidades: ${reportData.total_entidades || 'N/A'}
- Quejas del Día: ${reportData.quejas_hoy || 'N/A'}
- Quejas del Mes: ${reportData.quejas_mes_actual || 'N/A'}
` : ''}

TRAZABILIDAD:
- Trace ID: ${traceId}
- Timestamp: ${timestamp}

---
Sistema de Quejas - Departamento de Boyacá
Generado automáticamente el ${formattedDate}
        `;

        const mailOptions = {
            from: process.env.EMAIL_FROM || process.env.EMAIL_USER,
            to: process.env.EMAIL_TO || process.env.EMAIL_USER,
            subject: subject,
            html: html,
            text: text
        };

        const result = await this.transporter.sendMail(mailOptions);
        
        console.log(`[EMAIL-SERVICE] Email enviado [TraceID: ${traceId}] [MessageID: ${result.messageId}]`);
        
        return {
            success: true,
            messageId: result.messageId,
            traceId: traceId
        };
    }
}

module.exports = new EmailService();
