package com.keyvaluestore.coordinator;

import com.keyvaluestore.clock.VectorClock;
import com.keyvaluestore.exception.QuorumNotReachedException;
import com.keyvaluestore.model.VersionedValue;
import com.keyvaluestore.storage.StorageNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinador del clúster: punto de entrada único para operaciones get/put.
 *
 * Responsabilidades (SRP):
 *   - Enrutar operaciones a los N nodos de réplica.
 *   - Orquestar las escrituras/lecturas concurrentes mediante un ExecutorService.
 *   - Aplicar la lógica de Quórum (W, R) y fallar rápido si no se alcanza.
 *   - Gestionar el Reloj Vectorial de coordinación para garantizar causalidad.
 *
 * Lo que NO hace el Coordinator (respetando SRP):
 *   - No sabe cómo almacenar datos (eso es InMemoryStorageNode).
 *   - No sabe comparar relojes (eso es VectorClock).
 *
 * Modelo de Quórum:
 *   N = número total de réplicas donde se intenta escribir/leer.
 *   W = mínimo de confirmaciones necesarias para una ESCRITURA exitosa.
 *   R = mínimo de respuestas necesarias para una LECTURA exitosa.
 *
 *   Invariante de consistencia fuerte: W + R > N garantiza al menos un nodo
 *   que tenga el dato más reciente en cualquier intersección de quórum.
 *   (Ej: N=3, W=2, R=2 → solapa al menos 1 nodo)
 */
public class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final List<StorageNode> nodos;
    private final int n;    // Número total de réplicas (N en la notación del libro)
    private final int w;    // Quórum de escritura (W)
    private final int r;    // Quórum de lectura  (R)
    private final String coordinadorId;

    /**
     * El Coordinator guarda el reloj vectorial más reciente que conoce para cada
     * clave. Esto sirve como base para construir el reloj de la próxima escritura.
     *
     * En un sistema real, este estado se propagaría via gossip protocol. Aquí lo
     * mantenemos en memoria como parte de la simulación.
     *
     * ConcurrentHashMap porque put y get del Coordinator se pueden llamar
     * concurrentemente desde múltiples hilos cliente.
     */
    private final ConcurrentHashMap<String, VectorClock> relojConocidoPorClave;

    /**
     * Pool de hilos para simular las llamadas concurrentes a los nodos.
     * En un sistema real serían llamadas RPC/HTTP; aquí son invocaciones directas
     * al objeto Java en hilos separados para simular la latencia de red.
     */
    private final ExecutorService executor;

    /** Timeout máximo que el Coordinator espera la respuesta de cada nodo (ms). */
    private static final long TIMEOUT_MS = 500;

    public Coordinator(List<StorageNode> nodos, int n, int w, int r, String coordinadorId) {
        validarParametros(nodos, n, w, r, coordinadorId);
        this.nodos                 = List.copyOf(nodos);
        this.n                     = n;
        this.w                     = w;
        this.r                     = r;
        this.coordinadorId         = coordinadorId;
        this.relojConocidoPorClave = new ConcurrentHashMap<>();
        // Usamos un pool de tamaño N: máximo necesitamos lanzar N tareas en paralelo.
        this.executor              = Executors.newFixedThreadPool(Math.max(n, nodos.size()));
    }

    /**
     * Escribe `value` para `key` en el clúster, requiriendo W confirmaciones.
     *
     * Flujo interno:
     *   1. Tomamos el último reloj conocido para esta clave (o un reloj vacío si es nueva).
     *   2. Incrementamos el contador del coordinador → establece causalidad con escrituras previas.
     *   3. Lanzamos la escritura de forma concurrente a TODOS los nodos disponibles.
     *   4. Contamos cuántos responden con éxito antes del timeout.
     *   5. Si el conteo < W, la escritura es inválida → QuorumNotReachedException.
     *   6. Si W se alcanzó, actualizamos el reloj conocido del coordinador.
     *
     * Nota sobre el paso 3: intentamos escribir en TODOS los nodos del clúster
     * (no solo en n primeros). En Dynamo real, los nodos se eligen por posición
     * en el anillo de hash consistente. Aquí omitimos el anillo (YAGNI) y
     * tratamos la lista completa como los N nodos preferentes.
     *
     * @throws QuorumNotReachedException si no se obtienen W respuestas exitosas.
     * @throws IllegalArgumentException  si key o value son nulos/vacíos.
     */
    public void put(String key, String value) {
        validarClave(key);
        if (value == null) {
            throw new IllegalArgumentException("El valor no puede ser nulo.");
        }

        // Paso 1+2: construir el nuevo reloj vectorial incrementando el del coordinador.
        VectorClock nuevoReloj = relojConocidoPorClave
            .getOrDefault(key, new VectorClock())
            .increment(coordinadorId);

        VersionedValue versionado = new VersionedValue(value, nuevoReloj);
        log.info("[PUT] Iniciando escritura de clave '{}' con valor '{}' (Quórum W={}/{})", key, value, w, n);

        // Paso 3: lanzar escrituras concurrentes a todos los nodos.
        List<Future<Boolean>> futures = new ArrayList<>();
        for (StorageNode nodo : nodos) {
            futures.add(executor.submit(() -> {
                try {
                    nodo.put(key, versionado);
                    return true;
                } catch (Exception e) {
                    // El nodo puede estar caído o lanzar error; se cuenta como fallo.
                    return false;
                }
            }));
        }

        // Paso 4: contar éxitos respetando el timeout por nodo.
        int exitosos = contarExitos(futures);

        // Paso 5: verificar quórum.
        if (exitosos < w) {
            log.error("[PUT] Quórum fallido para clave '{}'. Se requerían {} respuestas, pero se obtuvieron {}", key, w, exitosos);
            throw new QuorumNotReachedException(w, exitosos);
        }

        log.info("[PUT] Éxito. Clave '{}' guardada con Quórum alcanzado ({} respuestas recibidas).", key, exitosos);

        // Paso 6: persistir el reloj actualizado en el estado del coordinador.
        relojConocidoPorClave.put(key, nuevoReloj);
    }

    /**
     * Lee el valor más reciente para `key` consultando R nodos del clúster.
     *
     * Flujo interno:
     *   1. Lanzamos lecturas concurrentes a TODOS los nodos.
     *   2. Recolectamos las respuestas no vacías que lleguen antes del timeout.
     *   3. Si la cantidad de respuestas exitosas < R → QuorumNotReachedException.
     *   4. Entre las respuestas, elegimos el valor con el reloj vectorial más avanzado.
     *      En caso de conflicto (relojes CONCURRENT), devolvemos el primero encontrado
     *      y dejamos la resolución de conflicto como responsabilidad del cliente
     *      (patrón "last write wins" o lógica de negocio). El sistema garantiza
     *      detectar el conflicto gracias a VectorClock.isConcurrentWith().
     *
     * @return Optional con el VersionedValue más reciente, o empty si la clave no existe.
     * @throws QuorumNotReachedException si no se obtienen R respuestas exitosas.
     */
    public Optional<VersionedValue> get(String key) {
        validarClave(key);
        log.info("[GET] Iniciando lectura de clave '{}' (Quórum R={}/{})", key, r, n);

        List<Future<Optional<VersionedValue>>> futures = new ArrayList<>();
        for (StorageNode nodo : nodos) {
            futures.add(executor.submit(() -> {
                // Si el nodo falla (ej. tira IllegalStateException por estar caído),
                // no hacemos try-catch, dejamos que la excepción se propague al Future.
                return nodo.get(key);
            }));
        }

        // Recolectar respuestas no vacías (nodos que tienen la clave).
        List<VersionedValue> respuestas = new ArrayList<>();
        for (Future<Optional<VersionedValue>> future : futures) {
            try {
                Optional<VersionedValue> respuesta = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                respuesta.ifPresent(respuestas::add);
            } catch (Exception e) {
                // Timeout o excepción de nodo caído: ignoramos esta respuesta.
            }
        }

        /*
         * Para calcular el quórum de lectura, necesitamos al menos R nodos que
         * respondieron (con o sin valor). Pero si un nodo está caído, su Future
         * llegará vacío. Volvemos a contar los futures con resultado (incluyendo empty).
         *
         * Decisión de diseño: contamos como respuesta exitosa cualquier nodo que
         * respondió sin excepción, aunque haya devuelto Optional.empty() (clave inexistente).
         * Esto es coherente con Dynamo: un nodo disponible que no tiene la clave es una
         * respuesta válida de "no encontrado".
         */
        int nodosQueRespondieron = contarRespuestasExitosas(futures);
        if (nodosQueRespondieron < r) {
            log.error("[GET] Quórum fallido para clave '{}'. Se requerían {} respuestas, pero se obtuvieron {}", key, r, nodosQueRespondieron);
            throw new QuorumNotReachedException(r, nodosQueRespondieron);
        }

        if (respuestas.isEmpty()) {
            log.info("[GET] Quórum alcanzado, pero la clave '{}' no se encontró en ningún nodo.", key);
            return Optional.empty();
        }

        log.info("[GET] Quórum alcanzado para clave '{}'. Valores recolectados: {}", key, respuestas.size());

        // Elegir el valor con el reloj causalmente más avanzado.
        return Optional.of(elegirMasReciente(respuestas));
    }

    /**
     * Libera los recursos del ExecutorService. Debe llamarse cuando el clúster
     * simulado ya no sea necesario (ej. al final de los tests con @AfterEach).
     */
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Espera el resultado de cada Future y cuenta los que devolvieron `true`.
     * Se usa en put() para contar escrituras exitosas.
     */
    private int contarExitos(List<Future<Boolean>> futures) {
        int exitosos = 0;
        for (Future<Boolean> future : futures) {
            try {
                if (Boolean.TRUE.equals(future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS))) {
                    exitosos++;
                }
            } catch (Exception e) {
                // Timeout, InterruptedException o ExecutionException: cuenta como fallo.
            }
        }
        return exitosos;
    }

    /**
     * Cuenta cuántos Futures de lectura completaron sin excepción (independientemente
     * de si la clave existía o no en ese nodo).
     */
    private int contarRespuestasExitosas(List<Future<Optional<VersionedValue>>> futures) {
        int exitosos = 0;
        for (Future<Optional<VersionedValue>> future : futures) {
            try {
                future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                exitosos++;
            } catch (Exception e) {
                // Nodo caído o timeout: no cuenta.
            }
        }
        return exitosos;
    }

    /**
     * Selecciona el VersionedValue causalmente más avanzado de una lista de respuestas.
     *
     * Algoritmo de selección:
     *   - Se itera por la lista y se mantiene el "ganador actual".
     *   - Si el candidato tiene un reloj AFTER al ganador actual, pasa a ser el nuevo ganador.
     *   - Si el candidato es CONCURRENT (conflicto), se mantiene el ganador actual.
     *     En un sistema real, aquí se invocaría la función de resolución de conflictos
     *     o se retornarían ambos como "siblings" para que el cliente decida.
     *   - Si es BEFORE o EQUAL, el ganador no cambia.
     *
     * Esta lógica aplica correctamente el principio de causalidad de los VectorClocks.
     */
    private VersionedValue elegirMasReciente(List<VersionedValue> respuestas) {
        VersionedValue ganador = respuestas.get(0);
        for (int i = 1; i < respuestas.size(); i++) {
            VersionedValue candidato = respuestas.get(i);
            VectorClock.ComparisonResult resultado =
                candidato.getClock().compareTo(ganador.getClock());

            if (resultado == VectorClock.ComparisonResult.AFTER) {
                ganador = candidato;
            }
            // CONCURRENT, BEFORE, EQUAL → el ganador actual se mantiene.
        }
        return ganador;
    }

    private void validarParametros(List<StorageNode> nodos, int n, int w, int r, String coordinadorId) {
        if (nodos == null || nodos.isEmpty()) {
            throw new IllegalArgumentException("La lista de nodos no puede ser nula ni vacía.");
        }
        if (w < 1 || r < 1) {
            throw new IllegalArgumentException("W y R deben ser al menos 1.");
        }
        if (w > nodos.size() || r > nodos.size()) {
            throw new IllegalArgumentException(
                "W y R no pueden superar el número total de nodos disponibles."
            );
        }
        if (coordinadorId == null || coordinadorId.isBlank()) {
            throw new IllegalArgumentException("El coordinadorId no puede ser nulo ni vacío.");
        }
    }

    private void validarClave(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("La clave no puede ser nula ni vacía.");
        }
    }
}
