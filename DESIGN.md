# DESIGN.md — Key-Value Store Distribuido

## 1. Resumen de la Implementación

El proyecto se estructura en tres piezas fundamentales que simulan una base de datos distribuida en memoria:

- **Almacenamiento (`StorageNode`)**: Cada nodo funciona como un servidor de base de datos independiente. Usan memoria (`ConcurrentHashMap`) para guardar los datos de forma rápida y segura cuando múltiples hilos acceden al mismo tiempo. Además, tienen la capacidad de simular una "caída" (dejar de responder).
- **Coordinación (`Coordinator`)**: Es el "director de orquesta". Recibe las peticiones de los clientes y las distribuye entre los distintos nodos de almacenamiento. Se encarga de hacer peticiones en paralelo (usando hilos) y de verificar que suficientes nodos respondan correctamente (la lógica del "Quórum").
- **Control de Versiones (`VectorClock`)**: Como en un sistema distribuido los datos viajan por distintos servidores con demoras, este componente lleva la cuenta de qué versión de los datos es la más reciente. Esto asegura que nadie sobrescriba el trabajo de otro por accidente.

Al estar divididos de esta forma, cada componente tiene un trabajo único y bien definido, haciéndolo fácil de entender y probar sin requerir infraestructura externa.

---

### ¿Qué es una SSTable?

Imagina que tenés un cuaderno donde anotás pedidos. Cada vez que el cuaderno
se llena, lo archivás y abrís uno nuevo. Periódicamente juntás varios cuadernos
viejos en uno solo para no tener que buscar en 20 lugares. Eso son las SSTables.

Son importantísimas en producción porque guardan datos en disco de forma eficiente.
Pero para este challenge, **guardar en disco no agrega nada al comportamiento que
estamos evaluando**: el quórum, la detección de conflictos, la tolerancia a fallos.
Un `ConcurrentHashMap` (un diccionario thread-safe en memoria) hace exactamente
lo mismo a los efectos de los tests.

---

### ¿Qué es el Gossip Protocol?

Imagina que en una oficina, en vez de que el jefe mande un memo a todos,
cada empleado le cuenta las novedades a 2 o 3 compañeros al azar. En un rato,
todos saben lo que pasó sin que nadie haya tenido que hablar con todos.

Eso es el Gossip Protocol: los nodos del clúster se "chismean" entre sí para
saber quién está vivo y quién está caído, sin un coordinador central.

Para simularlo, alcanza con una línea de código:

```java
nodo3.setAvailable(false); // el nodo 3 "se cayó"
```

## 2. El Teorema CAP — ¿Disponibilidad o Consistencia?

### La idea central

El Teorema CAP dice que cuando una red distribuida tiene problemas
(y siempre los va a tener tarde o temprano), tenés que elegir qué preferir:

- **Consistencia**: todos los usuarios ven el mismo dato al mismo tiempo.
  Si escribo "precio = $100" en Buenos Aires, alguien en Tokyo lo ve instantáneamente.
- **Disponibilidad**: el sistema siempre responde, aunque algún servidor esté caído.
  Quizás el dato no es el más reciente, pero hay una respuesta.

**No podés tener ambas cosas perfectas cuando hay fallas de red.** Tenés que
elegir cuál sacrificar un poco.

---

### ¿Qué eligió este diseño?

Este sistema elige **Disponibilidad** (el modelo de Amazon Dynamo).

La lógica es: en una aplicación de fintech (como una billetera virtual o home banking), a veces es preferible 
que la app te permita ver tu historial de movimientos (aunque falte la transferencia 
de hace 2 segundos) antes que la app quede totalmente congelada bloqueando a todos 
los usuarios mientras espera confirmación del 100% de los servidores del banco.

Esto se llama **consistencia eventual**: el sistema garantiza que todos los
nodos van a *eventualmente* converger al mismo dato, pero no necesariamente de
forma instantánea.

---

### ¿Cómo se ve esto en el código?

La configuración `N=3, W=2, R=2` es donde se toma esa decisión:

| Parámetro | Significado |
|---|---|
| **N (Nodes)** = 3 | El dato se replica en 3 servidores de base de datos distintos |
| **W (Write Quorum)** = 2 | Para que una escritura sea "exitosa", bastan 2 de 3 confirmaciones |
| **R (Read Quorum)** = 2 | Para leer, se consultan 2 de 3 servidores |

Con W=2, si el tercer servidor está caído, **la escritura igual funciona**.
El sistema eligió disponibilidad sobre esperar la confirmación perfecta.

Además, con W+R > N (2+2 > 3), se garantiza que el nodo que confirmó la
escritura siempre va a estar entre los consultados en la lectura. Así se evita
leer datos desactualizados, logrando un balance entre disponibilidad y consistencia.

---

### ¿Qué pasa cuando dos personas editan lo mismo a la vez?

Aquí entran los **Relojes Vectoriales** (sección 3). El sistema no elige una versión al azar; en cambio, *detecta* el conflicto y **preserva todas las versiones concurrentes** (denominadas *siblings*). Esto permite que la aplicación decida cómo resolverlo — igual que Amazon hace con el carrito: si hay conflicto, se retornan ambos carritos para que el cliente realice el merge.

Además, el sistema implementa **Read Repair** para garantizar la convergencia de las réplicas de forma pasiva cada vez que se realiza una lectura.

---

## 3. Cómo se implementó la lógica de los Vector Clocks

### Primero: ¿qué problema resuelven los Vector Clocks?

Imagina una billetera virtual donde tuvieras tu sesión abierta en dos celulares desconectados de internet. 
En el celular A anotas un gasto o haces una operación a las **14:00**, y en el celular B haces otra transacción diferente **también a las 14:00**.
Cuando de repente ambos celulares consiguen señal, envían la información a la base de datos al mismo tiempo. ¿Cuál es el estado correcto de tu cuenta? 
Los relojes clásicos de sistema (basados en la hora del día) no sirven para desempatar esto ni saber con precisión milimétrica quién operó "después".

Los **Relojes Vectoriales** resuelven esto: cada servidor lleva la cuenta de
cuántas veces él y cada otro servidor han escrito. Cuando hay un conflicto,
se comparan esos contadores para saber si una escritura "vio" a la otra antes
de editar, o si ambas escribieron "a ciegas" sin saber de la otra.

```
El Servidor del Celular A anota su escritura → Reloj A: [A:1, B:0]
El Servidor del Celular B anota su escritura → Reloj B: [A:0, B:1]

Cuando la base de datos central compara ambos relojes:
  El Reloj A tiene un número mayor para el dispositivo A  (1 > 0)
  El Reloj B tiene un número mayor para el dispositivo B  (1 > 0)
  
  → Conclusión: Como se cruzaron y ninguna tiene todos los números más altos que la otra, hay un CONFLICTO (CONCURRENT).
```

---

### ¿Qué se implemento concretamente?

**Decisión 1: el reloj no se modifica, se reemplaza**

En vez de editar el reloj existente (lo cual causaría problemas cuando varios
hilos lo usan al mismo tiempo), cada operación crea un reloj nuevo:

```java
// En vez de modificar el reloj actual...
public VectorClock increment(String nodeId) {
    Map<String, Long> next = new HashMap<>(entries); // copia
    next.merge(nodeId, 1L, Long::sum);               // modifica la copia
    return new VectorClock(next);                    // devuelve objeto nuevo
}
```

Esto evita toda una categoría de bugs difíciles de encontrar sin necesitar
`synchronized` ni locks.

---

**Decisión 2: salida anticipada en la comparación**

Al comparar dos relojes, vamos revisando los contadores servidor por servidor. Si encontramos que el **Reloj A** tiene más escrituras anotadas para un servidor, pero el **Reloj B** tiene más escrituras anotadas para otro servidor distinto, ya sabemos de inmediato que hay un conflicto cruzado. Ninguno es 100% más nuevo que el otro, por lo que no hace falta seguir mirando el resto de los números:

```java
if (esteGanaEnAlguno && otroGanaEnAlguno) return ComparisonResult.CONCURRENT;
```

---

**Decisión 3: cuatro resultados posibles, no tres**

La comparación normal de números devuelve `menor`, `igual` o `mayor`.
Pero los Vector Clocks tienen **cuatro** casos:

| Resultado | Significado en el mundo real |
|---|---|
| `BEFORE` | La versión A ocurrió antes que B (causalidad clara) |
| `AFTER` | La versión A ocurrió después que B |
| `EQUAL` | Son idénticas |
| `CONCURRENT` | **¡Conflicto!** Ninguna vio a la otra antes de escribir |

Usar un enum en vez de un número hace que el código sea imposible de malinterpretar.

---

**Decisión 4: métodos que "leen como lenguaje natural"**

En vez de comparar el enum manualmente en cada test, la IA generó métodos
que expresan la intención directamente:

```java
assertTrue(relojA.isConcurrentWith(relojB));  // "¿hay conflicto?"
assertTrue(relojA.happensBefore(relojB));     // "¿A ocurrió antes que B?"
```

---

### El test que valida todo esto

El **Test 3** prueba exactamente el caso de la imagen del inicio:
dos versiones del mismo dato escritas sin conocerse entre sí.

```java
// D([s0, 1], [s1, 2]) vs D([s0, 2], [s1, 1])
VectorClock relojA = new VectorClock().withVersion("s0", 1).withVersion("s1", 2);
VectorClock relojB = new VectorClock().withVersion("s0", 2).withVersion("s1", 1);

// s0: A tiene 1, B tiene 2  →  B ganó en s0
// s1: A tiene 2, B tiene 1  →  A ganó en s1
// Como cada uno ganó en algo distinto → CONFLICTO

assertEquals(CONCURRENT, relojA.compareTo(relojB)); // ✅
assertEquals(CONCURRENT, relojB.compareTo(relojA)); // ✅ (es simétrico)
```

---

## 4. Convergencia de Réplicas (Read Repair)

En un sistema Dynamo, la consistencia eventual se logra mediante mecanismos que "reparan" los datos obsoletos. Este simulador implementa **Read Repair**:

1.  Al realizar un `get()`, el Coordinador consulta `R` nodos.
2.  Si detecta que algún nodo tiene una versión causalmente anterior (`BEFORE`) a la más reciente encontrada, lanza una petición de escritura asíncrona hacia ese nodo.
3.  Esto asegura que, tras una lectura exitosa, el sistema tienda a la convergencia sin esperar a una escritura del cliente.

Si existen conflictos (siblings), el Read Repair intenta una "fusión" básica para propiciar la convergencia hacia un estado único, aunque la resolución definitiva sigue siendo responsabilidad del cliente.

---

## 5. Resumen visual de la arquitectura

```
           El cliente llama a:
        coordinator.put("key", "value")
        coordinator.get("key")
                    │
                    ▼
         ┌────────────────────┐
         │    COORDINATOR     │
         │                    │
         │  • ¿Cuántos nodos  │
         │    confirmaron?    │
         │  • ¿Se alcanzó el  │
         │    quórum (W/R)?   │
         │  • ¿Qué versión es │
         │    la más reciente?│
         └────────┬───────────┘
          ┌───────┼───────┐
          ▼       ▼       ▼
       [Nodo 1] [Nodo 2] [Nodo 3]
        ✅ OK    ✅ OK    ❌ Caído

   → 2 confirmaciones ≥ W=2  →  ¡Éxito!
```

| Clase | ¿Qué hace? |
|---|---|
| `VectorClock` | Lleva la cuenta de versiones y detecta conflictos |
| `VersionedValue` | Guarda el dato junto con su reloj vectorial |
| `StorageNode` | Define qué puede hacer un nodo (put/get) |
| `InMemoryStorageNode` | Nodo real: almacena datos en memoria, puede "caerse" |
| `Coordinator` | Orquesta todo: envía pedidos, cuenta éxitos, verifica quórum |
| `QuorumNotReachedException` | Error que se lanza si no hay suficientes nodos respondiendo |

---

## 5. Notas sobre el desarrollo con IA

Este proyecto y su documentación fueron diseñados y generados en colaboración con **Antigravity**

El uso del agente dentro del espacio de trabajo permitió:
- Diseñar la biblioteca de clases traduciendo los conceptos abstractos de una base de datos distribuida gigante (como DynamoDB) en un simulador compacto y local.
- Implementar algoritmos concurrentes complejos de forma segura mediante inmutabilidad (como los *Vector Clocks*).
- Refinar de manera iterativa y conversacional tanto el código fuente como esta misma arquitectura documental, adaptando ejemplos teóricos a escenarios reales y de negocio.

---

