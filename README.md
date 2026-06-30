# ARSW-kafka-event-driven
Laboratorio de Apache Kafka y Arquitecturas Orientadas por Eventos para el curso ARSW (Arquitecturas de Software). Este repositorio contiene la implementación, ejemplos y ejercicios relacionados con la construcción de sistemas distribuidos basados en eventos utilizando Apache Kafka.

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
 
| Parámetro            | Valor actual |
|----------------------|--------------|
| Topic                | `orders`     |
| Particiones          | 1            |
| Factor de replicación| 1            |
| Clave de mensaje     | Ninguna      |
| Retención            | 24 horas     |
 
### Riesgos identificados
 
| Parámetro            | Riesgo                                                                                                                                                 | Atributo de calidad afectado |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| 1 partición          | Solo un consumidor del grupo puede procesar mensajes en paralelo. Ante alto volumen de pedidos, se convierte en un cuello de botella irrecuperable.     | Escalabilidad                |
| Replicación 1        | Si el único broker falla, todos los eventos del topic se pierden permanentemente. No existe ninguna réplica de respaldo ni ISR.                        | Disponibilidad / Durabilidad |
| Sin clave            | Los mensajes se distribuyen en round-robin. Si en el futuro se agregan particiones, eventos del mismo pedido podrían quedar en particiones distintas, rompiendo el orden por `orderId`. | Consistencia / Mantenibilidad |
| Retención 24 horas   | Si un consumidor cae por más de 24 horas (incidente, mantenimiento), los eventos se pierden sin posibilidad de reprocesamiento. Tampoco es viable para analítica histórica ni auditoría. | Confiabilidad / Trazabilidad |
 
### Propuesta de mejoras para producción
 
| Parámetro            | Valor actual | Valor propuesto | Justificación |
|----------------------|--------------|-----------------|---------------|
| Particiones          | 1            | 3               | Permite que hasta 3 consumidores del mismo grupo trabajen en paralelo, aumentando el throughput y habilitando escalabilidad horizontal. |
| Factor de replicación| 1            | 3               | Tolera la caída de hasta 2 brokers sin pérdida de datos. Se recomienda además configurar `min.insync.replicas=2` para garantizar que al menos 2 réplicas confirmen la escritura. |
| Clave de mensaje     | Ninguna      | `orderId`       | Garantiza que todos los eventos de un mismo pedido lleguen a la misma partición, preservando el orden cronológico por pedido. |
| Retención            | 24 horas     | 7 días          | Permite que consumidores se recuperen de fallos prolongados, habilita reprocesamiento ante errores y soporta analítica e auditoría histórica. |
 
### Conclusión
 
Esta configuración puede ser aceptable en un entorno local de laboratorio, pero es inapropiada para producción. Los riesgos principales son la pérdida total de datos ante un fallo de broker (replicación 1), la incapacidad de escalar el procesamiento (1 partición) y la pérdida irreversible de eventos ante caídas de consumidores superiores a 24 horas. Las mejoras propuestas abordan directamente los atributos de escalabilidad, disponibilidad, confiabilidad y trazabilidad del sistema.
 
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
