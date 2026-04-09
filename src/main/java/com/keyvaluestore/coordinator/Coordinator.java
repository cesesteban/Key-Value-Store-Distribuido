package com.keyvaluestore.coordinator;

import com.keyvaluestore.clock.VectorClock;
import com.keyvaluestore.exception.QuorumNotReachedException;
import com.keyvaluestore.model.VersionedValue;
import com.keyvaluestore.storage.StorageNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinador del clúster: punto de entrada único para operaciones get/put.
 *
 * Mejoras:
 *   - Stateless: No almacena relojes; el cliente provee el contexto causal.
 *   - Quórum Asíncrono: Retorna en cuanto se alcanza W o R (baja latencia).
 *   - LWW (Last Write Wins): Resolución de conflictos basada en timestamps del reloj.
 *   - Retries: Reintentos automáticos en llamadas a nodos fallidos.
 *   - Tracing: Propagación de correlationId para observabilidad.
 */
public class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final List<StorageNode> nodos;
    private final int n;
    private final int w;
    private final int r;
    private final String coordinadorId;
    private final ExecutorService executor;

    private static final long TIMEOUT_MS = 500;
    private static final int MAX_RETRIES = 2;

    public Coordinator(List<StorageNode> nodos, int n, int w, int r, String coordinadorId) {
        validarParametros(nodos, n, w, r, coordinadorId);
        this.nodos         = List.copyOf(nodos);
        this.n             = n;
        this.w             = w;
        this.r             = r;
        this.coordinadorId = coordinadorId;
        this.executor      = Executors.newFixedThreadPool(Math.max(n, nodos.size()));
    }

    /**
     * Escribe `value` de forma stateless. 
     * El `contextClock` es el reloj que el cliente obtuvo en su última lectura.
     */
    public void put(String key, String value, VectorClock contextClock) {
        String correlationId = UUID.randomUUID().toString().substring(0, 8);
        validarClave(key);
        if (value == null) throw new IllegalArgumentException("El valor no puede ser nulo.");
        
        VectorClock clockToUse = contextClock != null ? contextClock : new VectorClock();
        VectorClock nuevoReloj = clockToUse.increment(coordinadorId);
        VersionedValue versionado = new VersionedValue(value, nuevoReloj);

        log.info("[{}] [PUT] Clave '{}' (W={}/{})", correlationId, key, w, n);

        CompletableFuture<Integer> quorumFuture = new CompletableFuture<>();
        AtomicInteger exitos = new AtomicInteger(0);
        AtomicInteger terminados = new AtomicInteger(0);

        for (StorageNode nodo : nodos) {
            ejecutarConRetries(() -> {
                nodo.put(key, versionado);
                return true;
            }, correlationId, nodo.getNodeId())
            .thenAccept(success -> {
                if (success) {
                    if (exitos.incrementAndGet() >= w) {
                        quorumFuture.complete(exitos.get());
                    }
                }
                if (terminados.incrementAndGet() >= nodos.size() && !quorumFuture.isDone()) {
                    quorumFuture.complete(exitos.get());
                }
            });
        }

        try {
            int totalExitos = quorumFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (totalExitos < w) {
                throw new QuorumNotReachedException(w, totalExitos);
            }
            log.info("[{}] [PUT] Éxito para '{}' ({} respuestas)", correlationId, key, totalExitos);
        } catch (QuorumNotReachedException e) {
            throw e;
        } catch (Exception e) {
            log.error("[{}] [PUT] Error o timeout alcanzado", correlationId);
            throw new QuorumNotReachedException(w, exitos.get());
        }
    }

    public void put(String key, String value) {
        put(key, value, null);
    }

    public List<VersionedValue> get(String key) {
        String correlationId = UUID.randomUUID().toString().substring(0, 8);
        validarClave(key);
        log.info("[{}] [GET] Clave '{}' (R={}/{})", correlationId, key, r, n);

        CompletableFuture<List<VersionedValue>> quorumFuture = new CompletableFuture<>();
        List<VersionedValue> todasLasRespuestas = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger exitos = new AtomicInteger(0);
        AtomicInteger terminados = new AtomicInteger(0);

        for (StorageNode nodo : nodos) {
            ejecutarConRetries(() -> nodo.get(key), correlationId, nodo.getNodeId())
            .thenAccept(opt -> {
                exitos.incrementAndGet();
                opt.ifPresent(todasLasRespuestas::add);
                
                if (exitos.get() >= r) {
                    quorumFuture.complete(new ArrayList<>(todasLasRespuestas));
                }
                if (terminados.incrementAndGet() >= nodos.size() && !quorumFuture.isDone()) {
                    quorumFuture.complete(new ArrayList<>(todasLasRespuestas));
                }
            });
        }

        try {
            List<VersionedValue> respuestas = quorumFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (exitos.get() < r) {
                throw new QuorumNotReachedException(r, exitos.get());
            }

            if (respuestas.isEmpty()) return Collections.emptyList();

            List<VersionedValue> ganadores = resolverConflictosLWW(respuestas);
            
            lanzarReadRepair(key, ganadores, correlationId);

            log.info("[{}] [GET] Éxito. Versiones: {}.", correlationId, ganadores.size());
            return ganadores;
        } catch (QuorumNotReachedException e) {
            throw e;
        } catch (Exception e) {
            throw new QuorumNotReachedException(r, exitos.get());
        }
    }

    private <T> CompletableFuture<T> ejecutarConRetries(RetryableTask<T> task, String correlationId, String nodeId) {
        return CompletableFuture.supplyAsync(() -> {
            int intento = 0;
            while (intento <= MAX_RETRIES) {
                try {
                    return task.execute();
                } catch (Exception e) {
                    intento++;
                    if (intento > MAX_RETRIES) break;
                    log.debug("[{}] Reintentando operación en nodo {} (intento {})", correlationId, nodeId, intento);
                }
            }
            throw new RuntimeException("Fallo tras reintentos");
        }, executor);
    }

    private List<VersionedValue> resolverConflictosLWW(List<VersionedValue> respuestas) {
        if (respuestas.size() <= 1) return respuestas;

        List<VersionedValue> causalmenteGanadores = new ArrayList<>();
        for (VersionedValue candidato : respuestas) {
            boolean dominado = false;
            for (VersionedValue otro : respuestas) {
                if (candidato != otro && candidato.getClock().happensBefore(otro.getClock())) {
                    dominado = true;
                    break;
                }
            }
            if (!dominado && causalmenteGanadores.stream().noneMatch(g -> g.getClock().equals(candidato.getClock()))) {
                causalmenteGanadores.add(candidato);
            }
        }

        if (causalmenteGanadores.size() <= 1) return causalmenteGanadores;

        log.debug("Conflicto detectado ({} siblings). Aplicando LWW...", causalmenteGanadores.size());
        VersionedValue lwwGanador = causalmenteGanadores.stream()
            .max(Comparator.comparingLong(v -> v.getClock().getMaxTimestamp()))
            .orElse(causalmenteGanadores.get(0));

        return List.of(lwwGanador);
    }

    private void lanzarReadRepair(String key, List<VersionedValue> ganadores, String correlationId) {
        if (ganadores.isEmpty()) return;
        VersionedValue mejorVersion = ganadores.get(0);

        for (StorageNode nodo : nodos) {
            executor.submit(() -> {
                try {
                    Optional<VersionedValue> current = nodo.get(key);
                    if (current.isEmpty() || current.get().getClock().happensBefore(mejorVersion.getClock())) {
                        log.debug("[{}] [READ REPAIR] Reparando nodo {}", correlationId, nodo.getNodeId());
                        nodo.put(key, mejorVersion);
                    }
                } catch (Exception ignored) {}
            });
        }
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) executor.shutdownNow();
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void validarParametros(List<StorageNode> nodos, int n, int w, int r, String coordinadorId) {
        if (nodos == null || nodos.isEmpty()) throw new IllegalArgumentException("Lista de nodos inválida.");
        if (w < 1 || r < 1 || w > nodos.size() || r > nodos.size()) throw new IllegalArgumentException("W/R inválidos.");
        if (coordinadorId == null || coordinadorId.isBlank()) throw new IllegalArgumentException("coordinadorId inválido.");
    }

    private void validarClave(String key) {
        if (key == null || key.isBlank()) throw new IllegalArgumentException("Clave inválida.");
    }

    @FunctionalInterface
    private interface RetryableTask<T> {
        T execute() throws Exception;
    }
}

