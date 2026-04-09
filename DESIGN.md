# DESIGN.md — Key-Value Store Distribuido (Versión Enterprise)

## 1. Resumen de la Implementación (Refactorizado)

El proyecto evoluciona de un simulador educativo a una biblioteca de alto rendimiento diseñada bajo principios de sistemas distribuidos "Cloud-Native":

- **Almacenamiento (`StorageNode`)**: Implementación reactiva con soporte para **latencia artificial** y fallas de red simuladas para testear condiciones de carrera.
- **Coordinación (`Coordinator`)**: Se rediseñó como un componente **Stateless**. Ya no almacena relojes internamente (lo cual limitaría la escalabilidad horizontal); ahora el cliente provee el contexto causal en cada petición.
- **Control de Versiones (`VectorClock`)**: Incorpora metadatos (timestamps) y **poda automática (Pruning)** de nodos antiguos basándose en tiempo, evitando la explosión de metadatos en clusters grandes.

---

## 2. Optimizaciones de Alto Rendimiento

### Quórum Asíncrono Reactivo
En la versión inicial, el Coordinador esperaba secuencialmente la respuesta de cada nodo. En esta versión, se utiliza `CompletableFuture` para lanzar todas las peticiones en paralelo.
**El sistema retorna éxito al cliente en cuanto se alcanza el quórum (W o R)**, disparando el resto de las operaciones (como Read Repair) en segundo plano. Esto reduce drásticamente la latencia percibida por el usuario.

### Resolución de Conflictos: LWW (Last Write Wins)
Aunque el sistema detecta conflictos causales mediante Vector Clocks, hemos implementado una política de **Last Write Wins** automática basada en el timestamp físico más alto dentro del reloj vectorial. Esto garantiza que ante una divergencia profunda (merging de dos branches concurrentes), el sistema converja hacia un estado único de forma determinista.

---

## 3. Modelo Stateless y Contexto Causal

En un sistema distribuido real (como DynamoDB o Cassandra), los coordinadores deben poder escalar horizontalmente. Para lograr esto, hemos movido el estado del reloj al cliente:

1.  **GET**: El cliente recibe el valor y su `VectorClock`.
2.  **MODIFY**: El cliente modifica el dato localmente.
3.  **PUT**: El cliente envía el nuevo valor **junto con el reloj que obtuvo en el paso 1**. 
4.  **COORDINATOR**: El coordinador incrementa el reloj recibido, garantizando que la nueva escritura sea causalmente posterior a la lectura original.

---

## 4. Observabilidad y Resiliencia

### Tracing (Correlation IDs)
Cada operación genera un `correlationId` único que se propaga a todos los hilos y nodos involucrados. Esto permite rastrear una petición completa a través de logs complejos, algo vital en entornos de microservicios.

### Reintentos (Retries)
Se implementó una política de reintentos asíncrona. Si un nodo falla por un problema transitorio (timeout corto), el Coordinador reintenta la operación automáticamente antes de dar por fallido el quórum.

### Poda de Relojes (Pruning)
Para evitar que los metadatos del sistema crezcan indefinidamente, el `VectorClock` mantiene un límite de **10 nodos**. Si se supera, se eliminan las entradas más antiguas basándose en sus timestamps de última actualización.

---

## 5. Resumen visual de la arquitectura Senior

```
            CLIENTE (Mantiene contexto causal)
                     │
         [GET] Clave -> Retorna (Valor + Reloj)
         [PUT] (Valor + RelojContexto)
                     │
                     ▼
          ┌────────────────────┐
          │  STATELESS COORD.  │  <─── Propaga CorrelationId
          │                    │
          │  • Async Quorum    │  <─── Retorna en cuanto W/R ok
          │  • LWW resolution  │
          │  • Automatic Retry │
          └────────┬───────────┘
           ┌───────┼───────┐
           ▼       ▼       ▼
        [Nodo 1] [Nodo 2] [Nodo 3]
         (Latencia variable y fallas asíncronas)
```

| Componente | Nivel de Madurez | Descripción |
|---|---|---|
| Concurrent Support | Senior | Quórum asíncrono no bloqueante. |
| Scalability | Enterprise | Stateless Coordinator ready para clusters. |
| Resilience | Production | Retries, Timeouts y Pruning incluidos. |
| Conflict Res. | Deterministic | LWW basado en metadatos del reloj. |

---

## 6. Notas sobre el desarrollo con IA

Este proyecto demuestra la capacidad de un agente de IA (**Antigravity**) para no solo implementar requerimientos, sino para **criticar y refactorizar** un sistema basándose en estándares de arquitectura de alto nivel (como los propuestos en los papers de Amazon Dynamo).
de una base de datos distribuida gigante (como DynamoDB) en un simulador compacto y local.
- Implementar algoritmos concurrentes complejos de forma segura mediante inmutabilidad (como los *Vector Clocks*).
- Refinar de manera iterativa y conversacional tanto el código fuente como esta misma arquitectura documental, adaptando ejemplos teóricos a escenarios reales y de negocio.

---
