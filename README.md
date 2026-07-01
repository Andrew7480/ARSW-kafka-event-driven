# ARSW-kafka-event-driven
Laboratorio de Apache Kafka y Arquitecturas Orientadas por Eventos para el curso ARSW (Arquitecturas de Software). Este repositorio contiene la implementación, ejemplos y ejercicios relacionados con la construcción de sistemas distribuidos basados en eventos utilizando Apache Kafka.

---
## Integrantes

- Andres Cardozo
- Juan David Gomez

---

## Actividad 1 — Análisis de comunicación (Cap. 1)

> Para una tienda en línea, clasifique qué procesos deberían ser síncronos, asíncronos o híbridos:
> consultar productos, crear pedido, validar pago, enviar notificación, actualizar analítica y registrar auditoría.
> Justifique brevemente su decisión.

| Proceso               | Tipo       | Justificación |
|-----------------------|------------|---------------|
| Consultar productos   | Síncrono   | El usuario espera la respuesta de inmediato para continuar navegando. No hay procesamiento posterior ni múltiples consumidores involucrados. |
| Crear pedido          | Híbrido    | La confirmación al usuario es síncrona (el cliente necesita saber que su pedido fue recibido), pero la cadena resultante (pago, inventario, notificación) se propaga de forma asíncrona. |
| Validar pago          | Híbrido    | La consulta inicial al gateway puede requerir respuesta inmediata para informar al usuario, pero el resultado dispara procesos asíncronos posteriores (facturación, reserva de inventario, notificación). |
| Enviar notificación   | Asíncrono  | No requiere respuesta inmediata ni bloquea el flujo principal. Se ejecuta en segundo plano; un fallo en la notificación no debe afectar la creación del pedido. |
| Actualizar analítica  | Asíncrono  | No necesita respuesta inmediata ni debe bloquear el flujo de negocio. Puede procesarse en diferido, soporta alto volumen y se beneficia del reprocesamiento. |
| Registrar auditoría   | Asíncrono  | Debe ocurrir sin bloquear el flujo principal. No requiere respuesta al usuario y se beneficia del reprocesamiento ante fallos o revisiones posteriores. |

### Conclusión

Los procesos que **impactan directamente la experiencia inmediata del usuario** (consultar, confirmar) deben ser síncronos. Los procesos de **soporte y consecuencias del negocio** (notificaciones, analítica, auditoría) son naturalmente asíncronos. Los procesos **core de negocio** como crear pedido y validar pago son híbridos: tienen una parte síncrona orientada al usuario y una parte asíncrona orientada a los sistemas internos.

---


## Actividad 2 — Decisiones de configuración (Cap. 2)

> Analice una configuración con un topic `orders`, una partición, factor de replicación 1, mensajes sin clave
> y retención de 24 horas. Identifique riesgos y proponga mejoras para un ambiente productivo.

### Configuración analizada

| Parámetro             | Valor actual |
|-----------------------|--------------|
| Topic                 | `orders`     |
| Particiones           | 1            |
| Factor de replicación | 1            |
| Clave de mensaje      | Ninguna      |
| Retención             | 24 horas     |

### Riesgos identificados

| Parámetro             | Riesgo                                                                                                                                                                                   | Atributo afectado            |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| 1 partición           | Solo un consumidor del grupo puede procesar mensajes en paralelo. Ante alto volumen de pedidos se convierte en un cuello de botella irrecuperable sin rediseño del topic.               | Escalabilidad                |
| Replicación 1         | Si el único broker falla, todos los eventos del topic se pierden permanentemente. No existe ninguna réplica de respaldo ni ISR.                                                          | Disponibilidad / Durabilidad |
| Sin clave             | Los mensajes se distribuyen en round-robin. Si en el futuro se agregan particiones, eventos del mismo pedido pueden quedar en particiones distintas, rompiendo el orden por `orderId`.   | Consistencia                 |
| Retención 24 horas    | Si un consumidor cae por más de 24 horas (incidente, mantenimiento), los eventos se pierden sin posibilidad de reprocesamiento. Tampoco es viable para analítica histórica ni auditoría. | Confiabilidad / Trazabilidad |

### Propuesta de mejoras para producción

| Parámetro             | Valor actual | Valor propuesto | Justificación |
|-----------------------|--------------|-----------------|---------------|
| Particiones           | 1            | 3               | Permite que hasta 3 consumidores del mismo grupo trabajen en paralelo, aumentando el throughput y habilitando escalabilidad horizontal. |
| Factor de replicación | 1            | 3               | Tolera la caída de hasta 2 brokers sin pérdida de datos. Se recomienda además `min.insync.replicas=2` para que al menos 2 réplicas confirmen cada escritura. |
| Clave de mensaje      | Ninguna      | `orderId`       | Garantiza que todos los eventos de un mismo pedido lleguen a la misma partición, preservando su orden cronológico. |
| Retención             | 24 horas     | 7 días          | Permite recuperar consumidores caídos, habilita reprocesamiento ante errores y soporta analítica e auditoría histórica. |

### Conclusión

Esta configuración es aceptable en un entorno local de laboratorio pero inapropiada para producción. Los riesgos principales son la pérdida total de datos ante fallo del broker (replicación 1), la incapacidad de escalar el procesamiento (1 partición) y la pérdida irreversible de eventos ante caídas de consumidores superiores a 24 horas. Las mejoras propuestas abordan directamente los atributos de escalabilidad, disponibilidad, confiabilidad y trazabilidad.

---

## Actividad 3 — Entorno de laboratorio (Cap. 3)

> Cree los topics `orders`, `payments` e `inventory`. Publique al menos cinco eventos JSON
> y verifique en Kafka UI el topic, partición, offset, clave y contenido.

### Levantar el entorno

```bash
docker compose up -d
docker ps
```

Kafka UI disponible en [http://localhost:8080](http://localhost:8080). Broker expuesto en `localhost:9092`.

![Kafka UI online](images/UIKafka.png)

### Crear los topics

```
docker exec -it arsw-kafka bash

/opt/kafka/bin/kafka-topics.sh --create --topic orders \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --create --topic payments \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --create --topic inventory \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1


/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

![Topics creados en terminal](images/TopicsTerminal.png)

### Publicar eventos JSON

**Topic `orders`:**

```
/opt/kafka/bin/kafka-console-producer.sh --topic orders \
  --bootstrap-server localhost:9092 \
  --property "parse.key=true" --property "key.separator=:"
```

```
ORD-1001:{"orderId":"ORD-1001","customerId":"CUS-01","total":120000,"status":"CREATED"}
ORD-1002:{"orderId":"ORD-1002","customerId":"CUS-02","total":85000,"status":"CREATED"}
ORD-1003:{"orderId":"ORD-1003","customerId":"CUS-03","total":310000,"status":"CREATED"}
```

![Eventos publicados en orders](images/Eventos.png)

**Topic `payments`:**

```
/opt/kafka/bin/kafka-console-producer.sh --topic payments \
  --bootstrap-server localhost:9092 \
  --property "parse.key=true" --property "key.separator=:"
```

```
ORD-1001:{"paymentId":"PAY-001","orderId":"ORD-1001","total":120000,"status":"APPROVED"}
ORD-1002:{"paymentId":"PAY-002","orderId":"ORD-1002","total":85000,"status":"APPROVED"}
ORD-1003:{"paymentId":"PAY-003","orderId":"ORD-1003","total":310000,"status":"REJECTED"}
```

![Eventos publicados en payments](images/EventosPayment.png)

### Consumir y verificar por consola

```
/opt/kafka/bin/kafka-console-consumer.sh --topic orders \
  --bootstrap-server localhost:9092 --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

![Verificación por consola](images/VerificacionTerminal.png)

### Verificación en Kafka UI



![Topics visibles en Kafka UI](images/TopicsUI.png)

### Observaciones

- Los mensajes con la misma clave (`orderId`) siempre se enrutan a la misma partición, garantizando orden por pedido.
- El topic `inventory` queda vacío por ahora; será utilizado en capítulos posteriores cuando el `inventory-service` consuma eventos de `orders` y publique su respuesta.
- Con un solo broker (`replication-factor 1`) el entorno es funcional para laboratorio pero no tolerante a fallos, como se analizó en la Actividad 2.

---

# Actividad 4 — Trazabilidad del evento (Cap. 4)

> Documente el recorrido del evento desde la solicitud HTTP hasta el consumidor. Indique topic, clave, partición,
> consumidor, Consumer Group y evidencia en Kafka UI.

### Componentes involucrados

| Componente | Clase | Responsabilidad |
|------------|-------|------------------|
| Endpoint REST | `OrderController` | Recibe la solicitud HTTP y construye el evento de dominio. |
| Evento de dominio | `OrderCreatedEvent` | Representa el hecho `order-created` (orderId, customerId, total, status, occurredAt). |
| Productor | `OrderEventProducer` | Publica el evento en el topic `orders` usando `orderId` como clave. |
| Topic | `orders` | Configurado con 3 particiones y factor de replicación 1 (`KafkaTopicConfig`). |
| Consumidor | `OrderEventConsumer` | Escucha el topic `orders` dentro del Consumer Group `inventory-service`. |

### Recorrido del evento

1. **Solicitud HTTP**: el cliente envía `POST /orders` con `customerId` y `total` en el body.
2. **Controller**: `OrderController.createOrder()` recibe el `CreateOrderRequest`, genera un `orderId` único (`"ORD-" + UUID.randomUUID()`) y construye el `OrderCreatedEvent` con estado `CREATED`.
3. **Producer**: `OrderEventProducer.publishOrderCreated(event)` invoca `kafkaTemplate.send("orders", event.getOrderId(), event)`, publicando el evento en el topic `orders` con `orderId` como clave de partición.
4. **Broker**: Kafka calcula la partición mediante `hash(orderId) % 3` (el topic tiene 3 particiones) y almacena el evento en el offset correspondiente. Todos los eventos de un mismo `orderId` quedarán siempre en la misma partición.
5. **Consumer**: `OrderEventConsumer`, registrado con `@KafkaListener(topics = "orders", groupId = "inventory-service")`, recibe el evento y lo imprime en consola: `Evento recibido en inventory-service: <orderId>`.

### Detalle de trazabilidad

| Atributo | Valor |
|----------|-------|
| Topic | `orders` |
| Clave (key) | `orderId` (ej. `ORD-3f2a1c9e-...`) |
| Partición | Determinada por `hash(orderId) % 3` |
| Consumer | `OrderEventConsumer.consume()` |
| Consumer Group | `inventory-service` |
| Serialización | `JsonSerializer` (producer) / `JsonDeserializer` (consumer) |

### Prueba

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS-01","total":120000}'
```

![Evento recibido en consola por inventory-service](images/TrazabilidadConsola.png)

### Evidencia en Kafka UI

![Mensaje publicado en el topic orders con clave, partición y offset](images/TrazabilidadKafkaUI.png)

### Observaciones

- El `orderId` como clave garantiza que, si en el futuro se agregan más eventos relacionados al mismo pedido (pagos, inventario), todos lleguen a la misma partición y se procesen en orden.
- El Consumer Group `inventory-service` es independiente del `group-id` por defecto (`order-service`) definido en `application.yml`; el `groupId` del `@KafkaListener` tiene prioridad y permite que distintos servicios lógicos consuman el mismo topic sin competir por las particiones.
- Toda la trayectoria (HTTP → Controller → Producer → Broker → Consumer) es la materialización práctica del flujo conceptual descrito en el Capítulo 1: el productor publica y continúa, sin conocer ni esperar al consumidor.

---

### Actividad 5 - Diseño del flujo 

> Proponga los eventos, topics, productores, consumidores, Consumer Groups y claves de particionamiento para el 
> flujo de compra. Justifique por qué no conviene usar un único topic global llamado events. 

## Eventos propuestos

| Evento | Servicio productor | Servicios consumidores |
|---------|--------------------|-------------------------|
| `order-created` | Order Service | Payment Service, Inventory Service, Analytics Service, Audit Service |
| `order-cancelled` | Order Service | Notification Service, Analytics Service, Audit Service |
| `payment-approved` | Payment Service | Invoice Service, Notification Service, Analytics Service, Audit Service |
| `payment-rejected` | Payment Service | Notification Service, Analytics Service, Audit Service |
| `inventory-reserved` | Inventory Service | Notification Service, Analytics Service, Audit Service |
| `inventory-rejected` | Inventory Service | Notification Service, Analytics Service, Audit Service |
| `invoice-generated` | Invoice Service | Notification Service, Analytics Service, Audit Service |
| `invoice-failed` | Invoice Service | Notification Service, Analytics Service, Audit Service |
| `notification-sent` | Notification Service | Analytics Service, Audit Service |
| `notification-failed` | Notification Service | Audit Service |


## Topics propuestos

| Topic | Eventos principales | Clave de particionamiento |
|--------|---------------------|---------------------------|
| `orders` | `order-created`, `order-cancelled` | `orderId` |
| `payments` | `payment-approved`, `payment-rejected` | `orderId` |
| `inventory` | `inventory-reserved`, `inventory-rejected` | `orderId` |
| `invoices` | `invoice-generated`, `invoice-failed` | `orderId` |
| `notifications` | `notification-sent`, `notification-failed` | `orderId` |
| `audit` | `audit-record-created` | `correlationId` |


## Productores

| Servicio | Topic donde publica |
|----------|----------------------|
| Order Service | `orders` |
| Payment Service | `payments` |
| Inventory Service | `inventory` |
| Invoice Service | `invoices` |
| Notification Service | `notifications` |
| Audit Service | `audit` |


## Consumidores

| Servicio | Topics que consume | Consumer Group |
|----------|--------------------| ----------------|
| Payment Service | `orders` | `payment-group` |
| Inventory Service | `orders` | `inventory-group` |
| Invoice Service | `payments` | `invoice-group` |
| Notification Service | `payments`, `inventory`, `invoices` | `notification-group` |
| Analytics Service | `orders`, `payments`, `inventory`, `invoices`, `notifications` | `analytics-group` |
| Audit Service | Todos los topics | `audit-group` |

Cada microservicio pertenece a un Consumer Group diferente para que todos reciban una copia independiente de los eventos.


## Claves de particionamiento
**`orderId`** para los topics:
  - `orders`
  - `payments`
  - `inventory`
  - `invoices`
  - `notifications`


**`correlationId`** para el topic `audit`.


## ¿Por qué no conviene usar un único topic llamado `events`?

No es recomendable utilizar un único topic global porque:

- Mezcla eventos de distintos dominios, dificultando su organización.
- Los consumidores tendrían que leer y filtrar muchos eventos que no necesitan.
- Reduce el rendimiento al procesar información innecesaria.
- Complica el mantenimiento y la evolución del sistema.
- Limita la posibilidad de configurar particiones, retención y permisos específicos para cada tipo de evento.
- Incrementa el acoplamiento entre los microservicios.

Por estas razones, es una mejor práctica separar los eventos en topics específicos (`orders`, `payments`, `inventory`, `invoices`, `notifications` y `audit`), lo que mejora la escalabilidad, el rendimiento y la mantenibilidad del sistema.


---

## Actividad 1 — Decisiones de comunicación (Cap. 9.1)

> Clasifique los siguientes procesos como REST, Kafka o arquitectura híbrida para una tienda en línea.
> Justificación basada en: respuesta inmediata, asincronía, múltiples consumidores y reprocesamiento.

| Proceso                    | Mecanismo     | Justificación |
|----------------------------|---------------|---------------|
| Consultar catálogo         | REST          | Requiere respuesta inmediata al usuario. No hay asincronía ni múltiples consumidores involucrados. |
| Crear pedido               | Híbrido       | El cliente necesita confirmación inmediata (REST), pero la creación dispara eventos `order-created` hacia payment-service, inventory-service y notification-service (Kafka). |
| Validar pago               | Híbrido       | La llamada al gateway de pago puede ser síncrona (REST), pero el resultado (`payment-approved` / `payment-rejected`) se publica en Kafka para que invoice-service, notification-service y audit-service lo consuman de forma independiente. |
| Enviar notificación        | Kafka         | No requiere respuesta inmediata. Es un proceso asíncrono de bajo acoplamiento; múltiples canales (correo, push, SMS) pueden consumir el mismo evento desde grupos distintos. |
| Actualizar analítica       | Kafka         | La analítica no requiere respuesta inmediata. Kafka permite alto volumen de eventos, múltiples consumidores y reprocesamiento histórico sin impacto en el flujo principal. |
| Registrar auditoría        | Kafka         | La auditoría es asíncrona por naturaleza. Permite reprocesamiento ante fallos, múltiples consumidores y no debe bloquear el flujo de negocio. |
| Consultar estado del pedido| REST          | El usuario espera una respuesta inmediata. La consulta es puntual y no genera efectos secundarios. |
| Actualizar inventario      | Híbrido       | El inventory-service reacciona de forma asíncrona al evento `order-created` (Kafka), pero la validación de disponibilidad de stock puede requerir una consulta síncrona interna antes de publicar `inventory-reserved` o `inventory-rejected`. |

### Conclusión

En una tienda en línea, la arquitectura óptima combina REST y Kafka según la naturaleza de cada proceso:

- **REST** para procesos que requieren respuesta inmediata al usuario (consultas, confirmaciones).
- **Kafka** para procesos asíncronos que no necesitan respuesta inmediata, que involucran múltiples consumidores o que se benefician del reprocesamiento (notificaciones, analítica, auditoría).
- **Híbrido** para procesos de negocio core como crear pedidos, validar pagos y actualizar inventario, donde el punto de entrada es síncrono pero las consecuencias se propagan de forma asíncrona mediante eventos.
