package com.keyvaluestore.clock;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Implementación inmutable de un Reloj Vectorial (Vector Clock).
 *
 * Un reloj vectorial es un mapa de { nodoId -> número de versión } que permite
 * determinar el orden causal entre eventos en un sistema distribuido.
 *
 * Por qué inmutabilidad: cada operación (increment, merge) devuelve un objeto NUEVO,
 * lo que elimina toda clase de bug de estado compartido en escenarios concurrentes,
 * sin necesidad de sincronización explícita en esta clase.
 *
 * Referencia: Capítulo 6 de "System Design Interview" — Alex Xu.
 */
public final class VectorClock {

    /**
     * Resultado posible al comparar dos relojes vectoriales.
     *
     *  BEFORE     → este reloj precede causalmente al otro (happened-before).
     *  AFTER      → este reloj sucede causalmente al otro.
     *  EQUAL      → son idénticos.
     *  CONCURRENT → existe un conflicto (siblings): cada uno tiene al menos
     *               un componente mayor que el otro, por lo que no se puede
     *               establecer un orden causal. Requiere resolución manual.
     */
    public enum ComparisonResult {
        BEFORE, AFTER, EQUAL, CONCURRENT
    }

    /** Mapa interno de nodo → versión. Nunca se muta una vez construido. */
    private final Map<String, Long> entries;

    /** Constructor del reloj vacío (punto de partida). */
    public VectorClock() {
        this.entries = Collections.emptyMap();
    }

    /** Constructor de copia interna usado por las operaciones funcionales. */
    private VectorClock(Map<String, Long> entries) {
        this.entries = Collections.unmodifiableMap(new HashMap<>(entries));
    }

    /**
     * Devuelve un nuevo reloj con el contador del nodo dado incrementado en 1.
     * El nodo se agrega si no existía previamente (inicio en 0 → pasa a 1).
     *
     * Se llama cuando un nodo procesa un evento propio: escritura, coordinación, etc.
     */
    public VectorClock increment(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("El identificador de nodo no puede ser nulo ni vacío.");
        }
        Map<String, Long> next = new HashMap<>(entries);
        next.merge(nodeId, 1L, Long::sum);
        return new VectorClock(next);
    }

    /**
     * Devuelve un nuevo reloj que resulta de hacer el merge con `other`.
     *
     * El merge toma el máximo componente a componente. Esto se usa en el receptor
     * de un mensaje para "ponerse al día" con el reloj del emisor antes de
     * incrementar su propio contador.
     *
     * Ejemplo:
     *   this  = { s0: 3, s1: 1 }
     *   other = { s0: 1, s1: 4, s2: 2 }
     *   merge = { s0: 3, s1: 4, s2: 2 }
     */
    public VectorClock merge(VectorClock other) {
        Map<String, Long> merged = new HashMap<>(entries);
        other.entries.forEach((nodeId, version) ->
            merged.merge(nodeId, version, Math::max)
        );
        return new VectorClock(merged);
    }

    /**
     * Fábrica de conveniencia para construir relojes en los tests.
     * Devuelve un nuevo reloj con la versión del nodo dado fijada al valor indicado
     * (sobrescribe si ya existía). No incrementa: asigna directamente.
     */
    public VectorClock withVersion(String nodeId, long version) {
        Map<String, Long> next = new HashMap<>(entries);
        next.put(nodeId, version);
        return new VectorClock(next);
    }

    /**
     * Compara este reloj con `other` y devuelve la relación causal entre ambos.
     *
     * Algoritmo:
     *   1. Reunir todos los nodos que aparecen en cualquiera de los dos relojes.
     *   2. Para cada nodo, comparar la versión en `this` vs `other` (0 si ausente).
     *   3. Si `this` gana en algún nodo y pierde en otro → CONCURRENT (conflicto).
     *   4. Si `this` gana en algún nodo y nunca pierde → AFTER.
     *   5. Si `this` nunca gana y pierde en algún nodo → BEFORE.
     *   6. Si nunca gana ni pierde → EQUAL.
     */
    public ComparisonResult compareTo(VectorClock other) {
        Set<String> todosLosNodos = new HashSet<>(entries.keySet());
        todosLosNodos.addAll(other.entries.keySet());

        boolean esteGanaEnAlguno  = false;
        boolean otroGanaEnAlguno  = false;

        for (String nodo : todosLosNodos) {
            long versionEste  = entries.getOrDefault(nodo, 0L);
            long versionOtro  = other.entries.getOrDefault(nodo, 0L);

            if (versionEste > versionOtro) esteGanaEnAlguno = true;
            if (versionOtro > versionEste) otroGanaEnAlguno = true;

            // Optimización: si ya sabemos que hay conflicto, salimos temprano.
            if (esteGanaEnAlguno && otroGanaEnAlguno) return ComparisonResult.CONCURRENT;
        }

        if (!esteGanaEnAlguno && !otroGanaEnAlguno) return ComparisonResult.EQUAL;
        if (esteGanaEnAlguno)                        return ComparisonResult.AFTER;
        return ComparisonResult.BEFORE;
    }

    /** Retorna true si este reloj precede causalmente a `other` (happened-before). */
    public boolean happensBefore(VectorClock other) {
        return compareTo(other) == ComparisonResult.BEFORE;
    }

    /** Retorna true si este reloj y `other` son concurrentes (hay conflicto). */
    public boolean isConcurrentWith(VectorClock other) {
        return compareTo(other) == ComparisonResult.CONCURRENT;
    }

    public Map<String, Long> getEntries() {
        return entries; // ya es unmodifiable
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
