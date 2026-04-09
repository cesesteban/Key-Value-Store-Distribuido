package com.keyvaluestore.coordinator;

import com.keyvaluestore.clock.VectorClock;
import com.keyvaluestore.exception.QuorumNotReachedException;
import com.keyvaluestore.model.VersionedValue;
import com.keyvaluestore.storage.StorageNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
     * @return Lista de VersionedValue. Estará vacía si la clave no existe. 
     *         Si contiene más de un elemento, es porque se detectaron conflictos concurrentes (siblings).
     * @throws QuorumNotReachedException si no se obtienen R respuestas exitosas.
     */
    public List<VersionedValue> get(String key) {
        validarClave(key);
        log.info("[GET] Iniciando lectura de clave '{}' (Quórum R={}/{})", key, r, n);

        List<Future<Optional<VersionedValue>>> futures = new ArrayList<>();
        // En un sistema real usaríamos solo N nodos preferentes del anillo de hash.
        for (StorageNode nodo : nodos) {
            futures.add(executor.submit(() -> nodo.get(key)));
        }

        // Recolectar todas las respuestas que lleguen a tiempo.
        List<VersionedValue> respuestas = new ArrayList<>();
        List<StorageNode> nodosQueRespondieronConValor = new ArrayList<>();
        List<StorageNode> nodosObsoletos = new ArrayList<>();

        for (int i = 0; i < nodos.size(); i++) {
            try {
                Optional<VersionedValue> respuesta = futures.get(i).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (respuesta.isPresent()) {
                    respuestas.add(respuesta.get());
                    nodosQueRespondieronConValor.add(nodos.get(i));
                }
            } catch (Exception e) {
                // Nodo caído o timeout: ignorar.
            }
        }

        int nodosTotalesQueRespondieron = contarRespuestasExitosas(futures);
        if (nodosTotalesQueRespondieron < r) {
            log.error("[GET] Quórum fallido para clave '{}'. Se requerían {} respuestas, pero se obtuvieron {}", key, r, nodosTotalesQueRespondieron);
            throw new QuorumNotReachedException(r, nodosTotalesQueRespondieron);
        }

        if (respuestas.isEmpty()) {
            log.info("[GET] Quórum alcanzado, clave '{}' no encontrada.", key);
            return Collections.emptyList();
        }

        // 1. Detectar las versiones más recientes (resolver conflictos/siblings).
        List<VersionedValue> ganadores = resolverConflictos(respuestas);
        
        // 2. Identificar nodos desactualizados para Read Repair.
        // Un nodo es obsoleto si su valor está causalmente ANTES de cualquiera de los ganadores.
        for (int i = 0; i < nodos.size(); i++) {
            final int index = i;
            try {
                Optional<VersionedValue> value = futures.get(i).get(0, TimeUnit.MILLISECONDS);
                if (value.isPresent()) {
                    boolean esObsoleto = ganadores.stream().anyMatch(g -> value.get().getClock().happensBefore(g.getClock()));
                    if (esObsoleto) {
                        nodosObsoletos.add(nodos.get(index));
                    }
                } else {
                    // Si el nodo respondió pero no tiene el valor, también es candidato a repair si otros sí lo tienen.
                    nodosObsoletos.add(nodos.get(index));
                }
            } catch (Exception ignored) {}
        }

        // 3. Lanzar Read Repair asíncrono.
        if (!nodosObsoletos.isEmpty()) {
            lanzarReadRepair(key, ganadores, nodosObsoletos);
        }

        log.info("[GET] Éxito. Versiones encontradas: {}. Nodos reparados en background: {}", ganadores.size(), nodosObsoletos.size());
        return ganadores;
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
     * Filtra la lista de respuestas conservando solo las versiones que no son
     * dominadas causalmente por ninguna otra.
     *
     * Si el resultado tiene más de un elemento, significa que hay un conflicto
     * concurrente (siblings) que no se puede resolver automáticamente.
     */
    private List<VersionedValue> resolverConflictos(List<VersionedValue> respuestas) {
        List<VersionedValue> ganadores = new ArrayList<>();
        for (VersionedValue candidato : respuestas) {
            boolean dominado = false;
            for (VersionedValue otro : respuestas) {
                if (candidato != otro && candidato.getClock().happensBefore(otro.getClock())) {
                    dominado = true;
                    break;
                }
            }
            // Evitar duplicados exactos en el resultado
            if (!dominado && ganadores.stream().noneMatch(g -> g.getClock().equals(candidato.getClock()))) {
                ganadores.add(candidato);
            }
        }
        return ganadores;
    }

    /**
     * Notifica a los nodos desactualizados con la versión más reciente encontrada.
     * Si hay múltiples siblings, enviamos una versión que es la "unión" de todos ellos
     * (merge de relojes) para propiciar la convergencia.
     */
    private void lanzarReadRepair(String key, List<VersionedValue> ganadores, List<StorageNode> obsoletos) {
        if (ganadores.isEmpty() || obsoletos.isEmpty()) return;

        // Si hay conflictos, creamos un valor que represente la unión para el repair.
        // En un sistema real esto podría ser más complejo.
        VersionedValue versionAReparar = ganadores.get(0);
        if (ganadores.size() > 1) {
            VectorClock relojMerged = ganadores.get(0).getClock();
            String valorMerged = ganadores.get(0).getValue();
            for (int i = 1; i < ganadores.size(); i++) {
                relojMerged = relojMerged.merge(ganadores.get(i).getClock());
                valorMerged += " + " + ganadores.get(i).getValue(); // Simulación de merge
            }
            versionAReparar = new VersionedValue(valorMerged + " (repaired)", relojMerged);
        }

        final VersionedValue finalVersion = versionAReparar;
        for (StorageNode nodo : obsoletos) {
            executor.submit(() -> {
                try {
                    log.debug("[READ REPAIR] Actualizando nodo {} para clave {}", nodo.getNodeId(), key);
                    nodo.put(key, finalVersion);
                } catch (Exception e) {
                    log.warn("[READ REPAIR] Falló actualización del nodo {}", nodo.getNodeId());
                }
            });
        }
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
