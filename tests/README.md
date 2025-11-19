# Tests del Email Consumer Service

## Descripción de los Tests

Este servicio contiene **5 tests unitarios** que validan el funcionamiento del consumer de emails sin necesidad de conexión real a Kafka ni envío de emails.

### Tests Implementados

#### Test 1: Conexión a Kafka
- ✅ Verifica que el consumer se conecta correctamente al broker
- ✅ Verifica que lanza error si falla la conexión
- **Mock:** KafkaJS

#### Test 2: Suscripción al topic
- ✅ Verifica suscripción a `email.notifications` con `fromBeginning: true`
- ✅ Verifica error en caso de fallo
- **Mock:** Consumer de Kafka

#### Test 3: Procesamiento de mensaje válido
- ✅ Procesa correctamente evento `REPORT_GENERATED`
- ✅ Procesa correctamente evento `report.generated` (formato alternativo)
- ✅ Verifica que se llama a `emailService.sendReportGeneratedEmail()` con datos correctos
- **Mock:** emailService

#### Test 4: Manejo de mensajes inválidos
- ✅ Maneja JSON malformado sin lanzar error
- ✅ Ignora eventos de tipo desconocido
- ✅ Verifica logging de errores
- **Mock:** console.error, emailService

#### Test 5: Commit y resolveOffset
- ✅ Hace commit del offset después de procesar exitosamente
- ✅ NO hace commit si el procesamiento falla
- ✅ Verifica que `resolveOffset()` se llama correctamente
- ✅ Verifica que el offset incrementa en +1 para el commit
- **Mock:** consumer.commitOffsets(), resolveOffset()

## Instalación de Dependencias

```powershell
npm install
```

Esto instalará:
- `jest@^29.7.0` - Framework de testing
- Todas las dependencias del servicio

## Ejecutar los Tests

### Ejecutar todos los tests
```powershell
npm test
```

### Ejecutar tests en modo watch (re-ejecuta al hacer cambios)
```powershell
npm run test:watch
```

### Ejecutar tests con cobertura de código
```powershell
npm run test:coverage
```

El reporte de cobertura se generará en `coverage/lcov-report/index.html`

## Estructura de Tests

```
Email_consumer_service_EPB/
├── tests/
│   ├── setup.js              # Configuración global de tests
│   └── emailConsumer.test.js # 5 tests unitarios
├── jest.config.json          # Configuración de Jest
└── package.json              # Scripts de testing
```

## Tecnologías Utilizadas

- **Jest 29.7**: Framework de testing
- **Mocks**: KafkaJS, emailService
- **Spies**: console.log, console.error

## Notas Importantes

- ✅ **NO requiere Kafka corriendo** - Se usa mock de KafkaJS
- ✅ **NO envía emails reales** - Se usa mock de emailService
- ✅ **NO conecta a ningún servicio externo**
- ✅ Tests 100% aislados y rápidos
- ✅ Ideal para CI/CD (GitHub Actions)

## Resultado Esperado

Al ejecutar `npm test` deberías ver:

```
PASS  tests/emailConsumer.test.js
  EmailConsumer - Tests Unitarios
    Test 1 - Conexión a Kafka
      ✓ Debe conectarse correctamente al broker de Kafka
      ✓ Debe lanzar error si falla la conexión a Kafka
    Test 2 - Suscripción al topic
      ✓ Debe suscribirse al topic "email.notifications" con fromBeginning: true
      ✓ Debe lanzar error si falla la suscripción
    Test 3 - Procesamiento de mensaje válido
      ✓ Debe procesar correctamente un evento REPORT_GENERATED
      ✓ Debe procesar correctamente un evento con eventType "report.generated"
    Test 4 - Manejo de mensajes con formato inválido
      ✓ Debe manejar mensaje con JSON malformado sin lanzar error
      ✓ Debe ignorar eventos de tipo desconocido
    Test 5 - Commit y resolveOffset
      ✓ Debe hacer commit del offset después de procesar exitosamente
      ✓ NO debe hacer commit si el procesamiento falla

Test Suites: 1 passed, 1 total
Tests:       10 passed, 10 total
```

## Debugging

Si un test falla, puedes habilitar los logs descomentando en `tests/setup.js`:

```javascript
// global.console.log = jest.fn();
// global.console.error = jest.fn();
```

Esto permitirá ver los console.log durante la ejecución de los tests.
