package com.keyvaluestore;

import com.keyvaluestore.coordinator.Coordinator;
import com.keyvaluestore.exception.QuorumNotReachedException;
import com.keyvaluestore.model.VersionedValue;
import com.keyvaluestore.storage.InMemoryStorageNode;
import com.keyvaluestore.storage.StorageNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests de integración del Coordinator.
 *
 * Validan el comportamiento end-to-end del sistema distribuido simulado:
 * quórum exitoso, tolerancia a fallos y comportamiento bajo quórum imposible.
 *
 * Configuración estándar de los tests: N=3, W=2, R=2
 *   - Cumple la invariante W + R > N (2+2 > 3) → consistencia fuerte.
 *   - Tolera la caída de 1 nodo (3 - 1 = 2 = W = R).
 */
@DisplayName("Coordinator — Quórum, tolerancia a fallos y versionado")
class CoordinatorTest {

    private InMemoryStorageNode nodo1;
    private InMemoryStorageNode nodo2;
    private InMemoryStorageNode nodo3;
    private Coordinator coordinator;

    @BeforeEach
    void setUp() {
        nodo1 = new InMemoryStorageNode("nodo-1");
        nodo2 = new InMemoryStorageNode("nodo-2");
        nodo3 = new InMemoryStorageNode("nodo-3");

        coordinator = new Coordinator(
            List.of(nodo1, nodo2, nodo3),
            3,            // N = 3 réplicas
            2,            // W = quórum de escritura
            2,            // R = quórum de lectura
            "coordinador-test"
        );
    }

    @AfterEach
    void tearDown() {
        // Liberar el ExecutorService para evitar fugas de recursos entre tests.
        coordinator.shutdown();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TEST 1: Escritura y lectura básica con quórum exitoso (todos los nodos activos)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[TEST 1] Escritura y lectura básica con quórum N=3, W=2, R=2")
    void escrituraYLecturaBasicaConQuorumExitoso() {
        // Escritura: los 3 nodos están disponibles → se alcanzan las 2 confirmaciones (W).
        coordinator.put("usuario:1001", "Alice");

        List<VersionedValue> resultado = coordinator.get("usuario:1001");

        assertFalse(resultado.isEmpty(), "La lista no debe estar vacía tras una escritura exitosa.");
        assertEquals(1, resultado.size(), "Debe haber exactamente una versión ganadora.");
        assertEquals("Alice", resultado.get(0).getValue(),
            "El valor recuperado debe coincidir con el escrito.");

        // Verificar que el VectorClock fue asignado correctamente al dato
        assertNotNull(resultado.get(0).getClock(),
            "El dato debe tener un reloj vectorial asignado.");
        assertFalse(resultado.get(0).getClock().getEntries().isEmpty(),
            "El reloj vectorial no debe estar vacío tras una escritura.");
    }

    @Test
    @DisplayName("[TEST 1 EXTENDIDO] Múltiples claves y lecturas independientes")
    void multiplesClavesIndependientes() {
        coordinator.put("producto:A", "Laptop");
        coordinator.put("producto:B", "Mouse");

        assertEquals("Laptop", coordinator.get("producto:A").get(0).getValue());
        assertEquals("Mouse",  coordinator.get("producto:B").get(0).getValue());
    }

    @Test
    @DisplayName("[TEST 1 EXTENDIDO] Escritura secuencial: el reloj avanza con cada put()")
    void escriturasSecuencialesAvanzanElReloj() {
        coordinator.put("config:timeout", "30");
        coordinator.put("config:timeout", "60");

        List<VersionedValue> resultados = coordinator.get("config:timeout");
        assertEquals(1, resultados.size());
        VersionedValue resultado = resultados.get(0);
        assertEquals("60", resultado.getValue(),
            "La segunda escritura debe sobreescribir a la primera.");

        // El reloj del coordinador debe haber avanzado al menos 2 pasos
        long versionCoordinador = resultado.getClock()
            .getEntries()
            .getOrDefault("coordinador-test", 0L);
        assertTrue(versionCoordinador >= 2,
            "Tras dos escrituras, el contador del coordinador debe ser ≥ 2, fue: " + versionCoordinador);
    }

    @Test
    @DisplayName("[TEST 1 EXTENDIDO] get() devuelve lista vacía para clave inexistente")
    void getDeClaveInexistenteDevuelveEmpty() {
        List<VersionedValue> resultado = coordinator.get("clave:que:no:existe");
        assertTrue(resultado.isEmpty(),
            "Una clave que nunca fue escrita debe devolver una lista vacía.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TEST 2: Tolerancia a fallos — 1 nodo de 3 está caído
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[TEST 2] Escritura exitosa con 1 nodo caído (2 de 3 disponibles ≥ W=2)")
    void escrituraExitosaConUnNodoCaido() {
        // Simulamos que el tercer nodo falla ANTES de la escritura.
        nodo3.setAvailable(false);

        // Con nodo1 y nodo2 disponibles, se alcanzan W=2 confirmaciones.
        assertDoesNotThrow(
            () -> coordinator.put("sesion:xyz", "activa"),
            "La escritura debe completarse exitosamente aun con un nodo caído."
        );
    }

    @Test
    @DisplayName("[TEST 2] Lectura exitosa con 1 nodo caído (2 de 3 disponibles ≥ R=2)")
    void lecturaExitosaConUnNodoCaido() {
        // Escribimos con los 3 nodos disponibles para asegurar que el dato está replicado.
        coordinator.put("sesion:abc", "token-123");

        // Ahora caemos el nodo 3.
        nodo3.setAvailable(false);

        List<VersionedValue> resultado = coordinator.get("sesion:abc");

        assertFalse(resultado.isEmpty(), "La lectura debe encontrar el dato en los 2 nodos disponibles.");
        assertEquals("token-123", resultado.get(0).getValue());
    }

    @Test
    @DisplayName("[TEST 2] Si el nodo caído no tenía el dato, la lectura sigue siendo correcta")
    void lecturaCorrectaCuandoNodoCaidoNoTeniaElDato() {
        // Escribimos SIN el nodo 2 disponible (cae antes de la escritura).
        nodo2.setAvailable(false);
        coordinator.put("clave:efimera", "valorX");

        // Recuperamos el nodo 2 y caemos el nodo 1.
        nodo2.setAvailable(true);
        nodo1.setAvailable(false);

        // Nodo 2 no tiene el dato (nunca lo recibió), Nodo 3 sí lo tiene.
        // Con Nodo 2 (vacío) + Nodo 3 (con dato) → R=2 se cumple.
        List<VersionedValue> resultado = coordinator.get("clave:efimera");

        assertFalse(resultado.isEmpty(), "Al menos un nodo respondiente debe tener el dato.");
        assertEquals("valorX", resultado.get(0).getValue());
    }

    @Test
    @DisplayName("[TEST 2] Escritura falla si los nodos caídos impiden alcanzar W (0 de 3 disponibles)")
    void escrituraFallaSiNoHayQuorum() {
        nodo1.setAvailable(false);
        nodo2.setAvailable(false);
        nodo3.setAvailable(false);

        QuorumNotReachedException ex = assertThrows(
            QuorumNotReachedException.class,
            () -> coordinator.put("clave:critica", "valorCritico"),
            "Con 0 nodos disponibles, debe lanzar QuorumNotReachedException."
        );

        // Verificar metadata de la excepción
        assertEquals(2, ex.getRequerido(), "El quórum requerido debe ser W=2.");
        assertEquals(0, ex.getObtenido(),  "Los nodos exitosos deben ser 0.");
    }

    @Test
    @DisplayName("[TEST 2] Lectura falla si los nodos caídos impiden alcanzar R")
    void lecturaFallaSiNoHayQuorum() {
        coordinator.put("dato:importante", "valor");

        // Caer 2 de 3 nodos → sólo 1 puede responder, pero R=2.
        nodo1.setAvailable(false);
        nodo2.setAvailable(false);

        assertThrows(
            QuorumNotReachedException.class,
            () -> coordinator.get("dato:importante"),
            "Con sólo 1 nodo disponible y R=2, debe lanzar QuorumNotReachedException."
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests de validación defensiva (diseño defensivo)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("put() lanza excepción si la clave es nula")
    void putConClaveNulaLanzaExcepcion() {
        assertThrows(IllegalArgumentException.class,
            () -> coordinator.put(null, "valor"));
    }

    @Test
    @DisplayName("put() lanza excepción si el valor es nulo")
    void putConValorNuloLanzaExcepcion() {
        assertThrows(IllegalArgumentException.class,
            () -> coordinator.put("clave:valida", null));
    }

    @Test
    @DisplayName("get() lanza excepción si la clave es nula")
    void getConClaveNulaLanzaExcepcion() {
        assertThrows(IllegalArgumentException.class,
            () -> coordinator.get(null));
    }

    @Test
    @DisplayName("Constructor lanza excepción si W supera el número de nodos")
    void constructorLanzaExcepcionSiWEsImposible() {
        List<StorageNode> dosNodos = List.of(nodo1, nodo2);
        assertThrows(IllegalArgumentException.class,
            () -> new Coordinator(dosNodos, 2, 3, 1, "coord")); // W=3 > nodos=2
    }

    // ─────────────────────────────────────────────────────────────────────────
    // NUEVOS TESTS: Preservación de Siblings y Read Repair
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[CONFLICTOS] Preservación de Siblings ante escrituras concurrentes")
    void preservacionDeSiblings() {
        // Simulamos una situación donde dos nodos reciben escrituras divergentes.
        // Esto es difícil de hacer con coordinator.put() porque él centraliza el reloj.
        // Lo forzamos inyectando directamente en los nodos.

        VersionedValue v1 = new VersionedValue("Valor A", 
            new com.keyvaluestore.clock.VectorClock().withVersion("nodo-1", 1));
        VersionedValue v2 = new VersionedValue("Valor B", 
            new com.keyvaluestore.clock.VectorClock().withVersion("nodo-2", 1));

        nodo1.put("conflicto", v1);
        nodo2.put("conflicto", v2);
        // nodo3 queda vacío para esta clave.

        // Al leer con R=2, si consulta nodo1 y nodo2, encontrará versiones concurrentes.
        List<VersionedValue> resultados = coordinator.get("conflicto");

        assertEquals(2, resultados.size(), "Deben preservarse ambas versiones concurrentes (siblings).");
        Set<String> valores = resultados.stream().map(VersionedValue::getValue).collect(java.util.stream.Collectors.toSet());
        assertTrue(valores.contains("Valor A"));
        assertTrue(valores.contains("Valor B"));
    }

    @Test
    @DisplayName("[CONVERGENCIA] Read Repair actualiza nodos obsoletos")
    void readRepairActualizaNodosObsoletos() throws InterruptedException {
        // 1. Escribimos un dato inicial en todos.
        coordinator.put("convergencia", "v1");

        // 2. Caemos el nodo 3 y actualizamos a v2.
        nodo3.setAvailable(false);
        coordinator.put("convergencia", "v2");
        nodo3.setAvailable(true); // Nodo 3 vuelve, pero tiene "v1" (obsoleto).

        // 3. Realizamos un GET. El quórum incluirá al nodo 3.
        // El coordinador detectará que nodo 3 es obsoleto y lanzará Read Repair.
        List<VersionedValue> resultados = coordinator.get("convergencia");
        assertEquals("v2", resultados.get(0).getValue());

        // 4. Pequeña espera para que el hilo de background del Read Repair complete.
        Thread.sleep(100);

        // 5. Verificar que nodo 3 ahora tiene "v2".
        VersionedValue valorNodo3 = nodo3.get("convergencia").orElseThrow();
        assertEquals("v2", valorNodo3.getValue(), "El nodo 3 debería haber sido reparado con v2.");
    }
}
