package com.keyvaluestore.model;

import com.keyvaluestore.clock.VectorClock;

/**
 * Representa un valor almacenado junto con su versión (expresada como VectorClock).
 *
 * La combinación valor + reloj vectorial es la unidad mínima de dato en el sistema.
 * Al ser inmutable, puede leerse concurrentemente por múltiples hilos sin riesgo.
 *
 * Equivale a la notación D([S1, v1], [S2, v2]) del libro de Alex Xu.
 */
public final class VersionedValue {

    private final String value;
    private final VectorClock clock;

    public VersionedValue(String value, VectorClock clock) {
        if (value == null) {
            throw new IllegalArgumentException("El valor no puede ser nulo.");
        }
        if (clock == null) {
            throw new IllegalArgumentException("El reloj vectorial no puede ser nulo.");
        }
        this.value = value;
        this.clock  = clock;
    }

    public String getValue() {
      return value; 
    }
    public VectorClock getClock() { 
      return clock;  
    }

    @Override
    public String toString() {
        return "VersionedValue{value='" + value + "', clock=" + clock + "}";
    }
}
