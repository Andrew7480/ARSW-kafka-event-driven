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
