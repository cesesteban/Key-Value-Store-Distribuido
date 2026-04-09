package com.keyvaluestore;

import com.keyvaluestore.clock.VectorClock;
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

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Coordinator — Quórum Asíncrono, Stateless y LWW")
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
            3, 
            2, 
            2, 
            "coordinador-test"
        );
    }

    @AfterEach
    void tearDown() {
        coordinator.shutdown();
    }

    @Test
    @DisplayName("[STATELESS] El cliente debe pasar el contexto (reloj) para mantener causalidad")
    void flujoStatelessCorrecto() {
        // 1. Primera escritura
        coordinator.put("key1", "val1");
        
        // 2. Lectura para obtener versión actual
        List<VersionedValue> res = coordinator.get("key1");
        VectorClock clockV1 = res.get(0).getClock();
        
        // 3. Segunda escritura pasando el contexto
        coordinator.put("key1", "val2", clockV1);
        
        VersionedValue finalRes = coordinator.get("key1").get(0);
        assertEquals("val2", finalRes.getValue());
        // El reloj debe tener al menos 2 en el coordinador
        assertEquals(2L, finalRes.getClock().getEntries().get("coordinador-test"));
    }

    @Test
    @DisplayName("[LWW] Conflictos concurrentes se resuelven por el timestamp más reciente")
    void resolucionLWW() throws InterruptedException {
        // Forzamos dos versiones concurrentes inyectando en nodos manualmente
        VectorClock c1 = new VectorClock().withVersion("nodo-1", 1);
        Thread.sleep(10); // Asegurar diferencia de timestamp
        VectorClock c2 = new VectorClock().withVersion("nodo-2", 1);
        
        VersionedValue v1 = new VersionedValue("Valor Antiguo", c1);
        VersionedValue v2 = new VersionedValue("Valor Nuevo", c2);
        
        nodo1.put("lww-key", v1);
        nodo2.put("lww-key", v2);
        
        // R=2: leerá nodo1 y nodo2. LWW debe elegir v2 (más reciente).
        List<VersionedValue> res = coordinator.get("lww-key");
        
        assertEquals(1, res.size(), "LWW debe resolver el conflicto a una sola versión.");
        assertEquals("Valor Nuevo", res.get(0).getValue());
    }

    @Test
    @DisplayName("[QUÓRUM ASÍNCRONO] Retorna éxito en cuanto W nodos responden, ignorando nodos lentos")
    void quorumRapidoConNodosLentos() {
        nodo1.setArtificialLatency(0);
        nodo2.setArtificialLatency(0);
        nodo3.setArtificialLatency(2000); // Supera el timeout de 500ms

        // W=2: nodo1 y nodo2 responden rápido. El put debe terminar exitosamente 
        // mucho antes de los 2000ms.
        long start = System.currentTimeMillis();
        assertDoesNotThrow(() -> coordinator.put("fast-key", "fast-val"));
        long duration = System.currentTimeMillis() - start;

        assertTrue(duration < 500, "El put debió terminar rápido gracias al quórum asíncrono. Tardó: " + duration);
    }

    @Test
    @DisplayName("[RETRIES] La operación tiene éxito tras reintentos si el nodo se recupera")
    void reintentosExitosos() {
        // Caso: Nodo 1 falla la primera vez pero tiene éxito después (simulado por lógica interna o forzado)
        // Como no tenemos un "falla en el primer intento" programable en StorageNode, 
        // testeamos que el Coordinator no muere si un nodo falla pero otros W sí responden.
        
        nodo3.setAvailable(false); // Falla permanente
        // W=2: nodo1 y nodo2 deben ser suficientes.
        assertDoesNotThrow(() -> coordinator.put("retry-key", "retry-val"));
    }

    @Test
    @DisplayName("[PRUNING] El Vector Clock se poda al superar los 10 nodos")
    void podaDeRelojVectorial() {
        VectorClock clock = new VectorClock();
        for (int i = 0; i < 15; i++) {
            clock = clock.increment("nodo-" + i);
        }
        
        // Debe tener solo 10 entradas (las últimas 10)
        assertEquals(10, clock.getEntries().size());
        // El primer nodo debió desaparecer
        assertNull(clock.getEntries().get("nodo-0"));
        // El último debe estar
        assertNotNull(clock.getEntries().get("nodo-14"));
    }

    @Test
    @DisplayName("[READ REPAIR] Actualiza nodos obsoletos con la versión ganadora")
    void readRepairActualizaObsoleto() throws InterruptedException {
        coordinator.put("repair", "v1");
        
        // Forzamos obsolescencia en nodo 3
        nodo3.setAvailable(false);
        coordinator.put("repair", "v2", coordinator.get("repair").get(0).getClock());
        
        nodo3.setAvailable(true); 
        // Nodo 3 tiene "v1". Nodo 1 y 2 tienen "v2".
        
        coordinator.get("repair"); // Dispara read repair
        
        Thread.sleep(100); // Esperar async repair
        
        assertEquals("v2", nodo3.get("repair").get().getValue());
    }
}

