package com.keyvaluestore.storage;

import com.keyvaluestore.model.VersionedValue;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementación en memoria del StorageNode usando estructuras thread-safe.
 *
 * ConcurrentHashMap garantiza seguridad en entornos multihilo:
 *   - Las lecturas no bloquean.
 *   - Las escrituras bloquean SÓLO el segmento (bucket) afectado, no el mapa completo.
 *   - No hay necesidad de synchronized externo para operaciones put/get simples.
 *
 * AtomicBoolean para `available` asegura que la flag de disponibilidad se lee/escribe
 * de forma atómica sin necesidad de volatile manual.
 *
 * Esta clase simula un nodo físico de un clúster Dynamo/Cassandra:
 * en producción, en vez de un HashMap, habría una engine de almacenamiento real
 * (ej. LSM-tree con SSTables y Memtable). Aquí nos limitamos a la simulación
 * en memoria por el principio YAGNI.
 */
public class InMemoryStorageNode implements StorageNode {

    private static final Logger log = LoggerFactory.getLogger(InMemoryStorageNode.class);

    private final String nodeId;

    /** Almacén principal de datos: clave → valor versionado. */
    private final ConcurrentHashMap<String, VersionedValue> store;

    /**
     * Bandera de disponibilidad. Cuando es false, el nodo simula estar caído:
     * lanza una excepción en cualquier operación, igual que haría un timeout
     * de red en un sistema real.
     */
    private final AtomicBoolean available;

    public InMemoryStorageNode(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("El nodeId no puede ser nulo ni vacío.");
        }
        this.nodeId    = nodeId;
        this.store     = new ConcurrentHashMap<>();
        this.available = new AtomicBoolean(true);
    }

    @Override
    public void put(String key, VersionedValue value) {
        verificarDisponibilidad();
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("La clave no puede ser nula ni vacía.");
        }
        if (value == null) {
            throw new IllegalArgumentException("El VersionedValue no puede ser nulo.");
        }
        store.put(key, value);
        log.debug("Nodo [{}] - Guardó clave: {}", nodeId, key);
    }

    @Override
    public Optional<VersionedValue> get(String key) {
        verificarDisponibilidad();
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("La clave no puede ser nula ni vacía.");
        }
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

    /**
     * Permite simular la caída y recuperación de un nodo desde los tests.
     * En un sistema real, esto lo controlaría el health-check o el gossip protocol.
     */
    public void setAvailable(boolean disponible) {
        available.set(disponible);
    }

    /**
     * Punto de falla de diseño defensivo central:
     * si el nodo no está disponible, lanzamos una excepción de runtime que el
     * Coordinator capturará y contabilizará como fallo de nodo.
     */
    private void verificarDisponibilidad() {
        if (!available.get()) {
            throw new IllegalStateException(
                "El nodo '" + nodeId + "' no está disponible (simulación de fallo)."
            );
        }
    }
}
