package com.keyvaluestore.storage;

import com.keyvaluestore.model.VersionedValue;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementación en memoria del StorageNode usando estructuras thread-safe.
 *
 * Mejoras:
 *   - Simulación de latencia variable para probar quórum asíncrono.
 *   - Manejo de disponibilidad atómico.
 */
public class InMemoryStorageNode implements StorageNode {

    private static final Logger log = LoggerFactory.getLogger(InMemoryStorageNode.class);

    private final String nodeId;
    private final ConcurrentHashMap<String, VersionedValue> store;
    private final AtomicBoolean available;
    private final AtomicLong artificialLatencyMs;

    public InMemoryStorageNode(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("El nodeId no puede ser nulo ni vacío.");
        }
        this.nodeId              = nodeId;
        this.store               = new ConcurrentHashMap<>();
        this.available           = new AtomicBoolean(true);
        this.artificialLatencyMs = new AtomicLong(0);
    }

    @Override
    public void put(String key, VersionedValue value) {
        simularLatenciaYFalla();
        if (key == null || key.isBlank()) throw new IllegalArgumentException("Clave inválida.");
        if (value == null) throw new IllegalArgumentException("VersionedValue nulo.");
        
        store.put(key, value);
        log.debug("Nodo [{}] - Guardó clave: {}", nodeId, key);
    }

    @Override
    public Optional<VersionedValue> get(String key) {
        simularLatenciaYFalla();
        if (key == null || key.isBlank()) throw new IllegalArgumentException("Clave inválida.");
        return Optional.ofNullable(store.get(key));
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public boolean isAvailable() {
        return available.get();
    }

    public void setAvailable(boolean disponible) {
        available.set(disponible);
    }

    /**
     * Define una latencia artificial para las operaciones de este nodo.
     */
    public void setArtificialLatency(long ms) {
        this.artificialLatencyMs.set(ms);
    }

    private void simularLatenciaYFalla() {
        if (!available.get()) {
            throw new RuntimeException("Node " + nodeId + " is UNREACHABLE (Simulated Network Failure)");
        }
        
        long latency = artificialLatencyMs.get();
        if (latency > 0) {
            try {
                Thread.sleep(latency);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Operation interrupted in node " + nodeId);
            }
        }
    }
}

