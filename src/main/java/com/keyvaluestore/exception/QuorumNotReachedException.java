package com.keyvaluestore.exception;

/**
 * Se lanza cuando el sistema no logra obtener respuestas exitosas de suficientes
 * nodos para cumplir con el quórum configurado (W para escrituras, R para lecturas).
 *
 * En un sistema Dynamo, si el quórum no se alcanza la operación falla rápido
 * ("fail-fast") para evitar lecturas/escrituras inconsistentes.
 */
public class QuorumNotReachedException extends RuntimeException {

    private final int requerido;
    private final int obtenido;

    public QuorumNotReachedException(int requerido, int obtenido) {
        super(String.format(
            "Quórum no alcanzado: se requerían %d respuestas exitosas pero sólo se obtuvieron %d.",
            requerido, obtenido
        ));
        this.requerido = requerido;
        this.obtenido  = obtenido;
    }

    public int getRequerido() { 
      return requerido; 
    }

    public int getObtenido() { 
      return obtenido;  
    }
}
