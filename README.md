# Key-Value Store Distribuido

Este proyecto es una simulación local de una base de datos distribuida en memoria (Key-Value Store) escrita en **Java 17**, inspirada en la arquitectura de Amazon Dynamo / Apache Cassandra.

## Características
* **Tolerancia a Fallos**: Uso de Quórum configurable (N, W, R) para lecturas y escrituras garantizando Alta Disponibilidad.
* **Control de Concurrencia**: Implementación inmutable y lock-free (thread-safe) de **Vector Clocks** para el versionado causal y la detección de colisiones de datos.
* **Trazabilidad**: Capa de logging en tiempo real con SLF4J y Logback para auditar las llamadas concurrentes a los nodos.
* **Manejo Defensivo**: Validador estricto y tipado de respuestas simulando timeouts y particiones de red con excepciones ricas como `QuorumNotReachedException`.

## Estructura del Proyecto

* `com.keyvaluestore.clock.*`: Lógica de resolución de Relojes Vectoriales en conflictos cruzados.
* `com.keyvaluestore.coordinator.*`: Controlador principal que balancea carga, consulta Nodos en paralelo usando multithreading y define si el Quórum se cumple.
* `com.keyvaluestore.storage.*`: Nodos individuales que simulan tener un motor de almacenamiento propio, permitiendo inyectarle cortes (simulación de caída de servidor).

Para ver arquitectura, principios de ingeniería (KISS, YAGNI) y contexto tecnológico avanzado, leer `DESIGN.md`.

## Uso y Pruebas
Para compilar y correr la suite completa de tests de la simulación de red garantizando que todo está robusto:

```bash
mvn clean test
```

Observarás los logs en la consola demostrando cómo los hilos paralelos logran W de N y toleran la pérdida del resto de los nodos, validando así el teorema CAP (priorizando disponibilidad AP).
