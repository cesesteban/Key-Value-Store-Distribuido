package com.keyvaluestore.storage;

import com.keyvaluestore.clock.VectorClock;
import com.keyvaluestore.model.VersionedValue;

import java.util.Optional;

/**
 * Contrato que debe cumplir cualquier nodo de almacenamiento del clúster.
 *
 * SRP (Single Responsibility Principle): esta interfaz define SOLO la
 * responsabilidad de persistir y recuperar datos versionados. No conoce nada
 * de quórum, coordinación ni relojes vectoriales (esos son problemas del Coordinator).
 *
 * Diseño defensivo: `get` devuelve Optional para que el llamador deba manejar
 * explícitamente el caso en que la clave no exista, evitando NullPointerExceptions
 * silenciosos.
 */
public interface StorageNode {

    /**
     * Persiste un valor con su reloj vectorial asociado para la clave dada.
     * Si ya existe un valor para esa clave, lo reemplaza con la nueva versión.
     *
     * @param key   clave no nula.
     * @param value valor del dato versionado.
     * @throws IllegalArgumentException si la clave es nula o vacía.
     */
    void put(String key, VersionedValue value);

    /**
     * Recupera el valor versionado almacenado para la clave dada.
     *
     * @param key clave a buscar.
     * @return Optional con el valor si existe, Optional.empty() si no.
     */
    Optional<VersionedValue> get(String key);

    /** Identificador único del nodo dentro del clúster. */
    String getNodeId();

    /**
     * Indica si el nodo está operativo. Se usa para simular fallos parciales:
     * un nodo con isAvailable() == false rechaza todas las operaciones,
     * permitiendo testear la tolerancia a fallos sin infraestructura real.
     */
    boolean isAvailable();
}
