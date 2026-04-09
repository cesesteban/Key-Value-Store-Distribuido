package com.keyvaluestore.clock;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Implementación inmutable de un Reloj Vectorial (Vector Clock) con soporte para poda.
 *
 * Un reloj vectorial es un mapa de { nodoId -> (versión, timestamp) } que permite
 * determinar el orden causal entre eventos en un sistema distribuido.
 *
 * Mejoras:
 *  - Soporte para metadatos (timestamps) por entrada.
 *  - Lógica de poda (pruning) para evitar crecimiento ilimitado (Límite: 10 nodos).
 *
 * Referencia: Capítulo 6 de "System Design Interview" — Alex Xu.
 */
public final class VectorClock {

    public enum ComparisonResult {
        BEFORE, AFTER, EQUAL, CONCURRENT
    }

    /**
     * Representa una entrada en el reloj: contador de versión y timestamp de la última actualización.
     */
    public record ClockEntry(long version, long timestamp) {}

    private final Map<String, ClockEntry> entries;
    private static final int MAX_NODES = 10;

    public VectorClock() {
        this.entries = Collections.emptyMap();
    }

    private VectorClock(Map<String, ClockEntry> entries) {
        this.entries = Collections.unmodifiableMap(new HashMap<>(entries));
    }

    /**
     * Incrementa la versión del nodo y actualiza su timestamp.
     * Aplica poda si se supera el límite de nodos.
     */
    public VectorClock increment(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("El identificador de nodo no puede ser nulo ni vacío.");
        }
        Map<String, ClockEntry> next = new HashMap<>(entries);
        long currentVersion = next.containsKey(nodeId) ? next.get(nodeId).version() : 0L;
        next.put(nodeId, new ClockEntry(currentVersion + 1, System.currentTimeMillis()));
        
        return new VectorClock(next).prune();
    }

    /**
     * Merge de dos relojes tomando el máximo de versiones y timestamps.
     */
    public VectorClock merge(VectorClock other) {
        Map<String, ClockEntry> merged = new HashMap<>(entries);
        other.entries.forEach((nodeId, otherEntry) -> {
            ClockEntry thisEntry = merged.get(nodeId);
            if (thisEntry == null) {
                merged.put(nodeId, otherEntry);
            } else {
                merged.put(nodeId, new ClockEntry(
                    Math.max(thisEntry.version(), otherEntry.version()),
                    Math.max(thisEntry.timestamp(), otherEntry.timestamp())
                ));
            }
        });
        return new VectorClock(merged).prune();
    }

    /**
     * Poda el reloj si supera MAX_NODES, eliminando los nodos con timestamps más antiguos.
     */
    private VectorClock prune() {
        if (entries.size() <= MAX_NODES) return this;

        List<Map.Entry<String, ClockEntry>> list = new ArrayList<>(entries.entrySet());
        list.sort(Comparator.comparingLong(e -> e.getValue().timestamp()));

        Map<String, ClockEntry> pruned = new HashMap<>(entries);
        for (int i = 0; i < list.size() - MAX_NODES; i++) {
            pruned.remove(list.get(i).getKey());
        }
        return new VectorClock(pruned);
    }

    public VectorClock withVersion(String nodeId, long version) {
        Map<String, ClockEntry> next = new HashMap<>(entries);
        next.put(nodeId, new ClockEntry(version, System.currentTimeMillis()));
        return new VectorClock(next);
    }

    /**
     * Compara causalmente ignorando los timestamps (quienes solo sirven para poda).
     */
    public ComparisonResult compareTo(VectorClock other) {
        Set<String> todosLosNodos = new HashSet<>(entries.keySet());
        todosLosNodos.addAll(other.entries.keySet());

        boolean esteGanaEnAlguno  = false;
        boolean otroGanaEnAlguno  = false;

        for (String nodo : todosLosNodos) {
            long vEste = entries.containsKey(nodo) ? entries.get(nodo).version() : 0L;
            long vOtro = other.entries.containsKey(nodo) ? other.entries.get(nodo).version() : 0L;

            if (vEste > vOtro) esteGanaEnAlguno = true;
            if (vOtro > vEste) otroGanaEnAlguno = true;

            if (esteGanaEnAlguno && otroGanaEnAlguno) return ComparisonResult.CONCURRENT;
        }

        if (!esteGanaEnAlguno && !otroGanaEnAlguno) return ComparisonResult.EQUAL;
        if (esteGanaEnAlguno)                        return ComparisonResult.AFTER;
        return ComparisonResult.BEFORE;
    }

    public boolean happensBefore(VectorClock other) {
        return compareTo(other) == ComparisonResult.BEFORE;
    }

    public boolean isConcurrentWith(VectorClock other) {
        return compareTo(other) == ComparisonResult.CONCURRENT;
    }

    /**
     * Devuelve el timestamp más reciente presente en el reloj. 
     * Útil para resolución de conflictos LWW (Last Write Wins).
     */
    public long getMaxTimestamp() {
        return entries.values().stream()
            .mapToLong(ClockEntry::timestamp)
            .max()
            .orElse(0L);
    }

    public Map<String, Long> getEntries() {
        Map<String, Long> versions = new HashMap<>();
        entries.forEach((k, v) -> versions.put(k, v.version()));
        return Collections.unmodifiableMap(versions);
    }

    @Override
    public String toString() {
        return "VectorClock" + entries;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof VectorClock other)) return false;
        return entries.equals(other.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }
}

