package com.keyvaluestore;

import com.keyvaluestore.clock.VectorClock;
import com.keyvaluestore.clock.VectorClock.ComparisonResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests unitarios para VectorClock.
 *
 * Cada test cubre una arista del algoritmo de comparación causal. El caso de
 * conflicto (CONCURRENT) es el más crítico en sistemas distribuidos porque indica
 * que dos escrituras ocurrieron sin conocerse mutuamente, lo que requiere
 * resolución manual.
 */
@DisplayName("VectorClock — Comparación causal y detección de conflictos")
class VectorClockTest {

    // ─────────────────────────────────────────────────────────────────────────
    // Casos básicos de relación causal (BEFORE / AFTER / EQUAL)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Un reloj vacío precede a cualquier reloj no vacío")
    void relojVacioPrecedeAlNoVacio() {
        VectorClock vacio     = new VectorClock();
        VectorClock conDatos  = new VectorClock().increment("s0");

        assertEquals(ComparisonResult.BEFORE, vacio.compareTo(conDatos));
        assertEquals(ComparisonResult.AFTER,  conDatos.compareTo(vacio));
    }

    @Test
    @DisplayName("Dos relojes vacíos son iguales")
    void dosRelojosVaciosSonIguales() {
        assertEquals(ComparisonResult.EQUAL, new VectorClock().compareTo(new VectorClock()));
    }

    @Test
    @DisplayName("Un reloj precede a otro cuando todos sus componentes son ≤ y al menos uno es <")
    void relojPrecedeCuandoTodosLosComponenteresMenoresOIgual() {
        // Secuencia causal: s0 escribe → s1 ve esa escritura y escribe → relojB > relojA
        VectorClock relojA = new VectorClock()
            .withVersion("s0", 1)
            .withVersion("s1", 1);

        VectorClock relojB = new VectorClock()
            .withVersion("s0", 1)
            .withVersion("s1", 2);  // s1 avanzó

        assertEquals(ComparisonResult.BEFORE, relojA.compareTo(relojB));
        assertEquals(ComparisonResult.AFTER,  relojB.compareTo(relojA));
        assertTrue(relojA.happensBefore(relojB));
        assertFalse(relojB.happensBefore(relojA));
    }

    @Test
    @DisplayName("Dos relojes idénticos son EQUAL, no CONCURRENT")
    void relojesIdentocosSonEqual() {
        VectorClock relojA = new VectorClock().withVersion("s0", 3).withVersion("s1", 2);
        VectorClock relojB = new VectorClock().withVersion("s0", 3).withVersion("s1", 2);

        assertEquals(ComparisonResult.EQUAL, relojA.compareTo(relojB));
        assertFalse(relojA.isConcurrentWith(relojB));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TEST 3 PRINCIPAL: Detección de conflictos CONCURRENT
    // Simula el ejemplo exacto del enunciado:
    //   D([s0, 1], [s1, 2])  vs  D([s0, 2], [s1, 1])
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[TEST 3] Detecta conflicto CONCURRENT: D([s0,1],[s1,2]) vs D([s0,2],[s1,1])")
    void detectaConflictoConcurrente() {
        /*
         * Escenario:
         *   - Versión A fue generada por un cliente que vio s0=1, s1=2.
         *     (s1 hizo más escrituras desde la perspectiva de ese cliente)
         *   - Versión B fue generada por otro cliente que vio s0=2, s1=1.
         *     (s0 hizo más escrituras desde la perspectiva de ese cliente)
         *
         *   Ninguna versión "vio" a la otra antes de escribir → escrituras concurrentes.
         *   La función de comparación DEBE devolver CONCURRENT en ambas direcciones.
         */
        VectorClock relojA = new VectorClock()
            .withVersion("s0", 1)
            .withVersion("s1", 2);

        VectorClock relojB = new VectorClock()
            .withVersion("s0", 2)
            .withVersion("s1", 1);

        // La relación es simétrica: ambos son CONCURRENT entre sí.
        assertEquals(ComparisonResult.CONCURRENT, relojA.compareTo(relojB),
            "A → B debe ser CONCURRENT porque A tiene s1>B[s1] pero A tiene s0<B[s0]");
        assertEquals(ComparisonResult.CONCURRENT, relojB.compareTo(relojA),
            "B → A también debe ser CONCURRENT (la relación es simétrica)");

        // Verificar via métodos de conveniencia
        assertTrue(relojA.isConcurrentWith(relojB));
        assertTrue(relojB.isConcurrentWith(relojA));

        // Confirmar que NO existe relación causal entre ellos
        assertFalse(relojA.happensBefore(relojB));
        assertFalse(relojB.happensBefore(relojA));
    }

    @Test
    @DisplayName("[TEST 3 EXTENDIDO] Conflicto con 3 nodos: D([s0,2],[s1,1],[s2,0]) vs D([s0,1],[s1,2],[s2,0])")
    void detectaConflictoConcurrenteConTresNodos() {
        VectorClock relojA = new VectorClock()
            .withVersion("s0", 2)
            .withVersion("s1", 1)
            .withVersion("s2", 0);

        VectorClock relojB = new VectorClock()
            .withVersion("s0", 1)
            .withVersion("s1", 2)
            .withVersion("s2", 0);

        // s2 es igual en ambos, pero s0 y s1 tienen el dominante cruzado → CONCURRENT
        assertEquals(ComparisonResult.CONCURRENT, relojA.compareTo(relojB));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests de increment y merge
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("increment() es inmutable: no modifica el reloj original")
    void incrementNoMutaElRelojOriginal() {
        VectorClock original    = new VectorClock().withVersion("s0", 5);
        VectorClock incrementado = original.increment("s0");

        // El original no cambia
        assertEquals(5L, original.getEntries().get("s0"));
        // El nuevo sí avanzó
        assertEquals(6L, incrementado.getEntries().get("s0"));
    }

    @Test
    @DisplayName("merge() toma el máximo componente a componente")
    void mergeTomaElMaximoComponente() {
        VectorClock relojA = new VectorClock().withVersion("s0", 3).withVersion("s1", 1);
        VectorClock relojB = new VectorClock().withVersion("s0", 1).withVersion("s1", 4).withVersion("s2", 2);

        VectorClock merged = relojA.merge(relojB);

        assertEquals(3L, merged.getEntries().get("s0")); // max(3,1)
        assertEquals(4L, merged.getEntries().get("s1")); // max(1,4)
        assertEquals(2L, merged.getEntries().get("s2")); // max(0,2) → nuevo nodo
    }

    @Test
    @DisplayName("increment() de un nodo inexistente lo inicializa en 1")
    void incrementInicializaNodoNuevo() {
        VectorClock reloj = new VectorClock().increment("nuevoNodo");
        assertEquals(1L, reloj.getEntries().get("nuevoNodo"));
    }

    @Test
    @DisplayName("increment() lanza excepción si el nodeId es nulo")
    void incrementLanzaExcepcionConNodeIdNulo() {
        assertThrows(IllegalArgumentException.class,
            () -> new VectorClock().increment(null));
    }

    @Test
    @DisplayName("increment() lanza excepción si el nodeId está vacío")
    void incrementLanzaExcepcionConNodeIdVacio() {
        assertThrows(IllegalArgumentException.class,
            () -> new VectorClock().increment("   "));
    }
}
