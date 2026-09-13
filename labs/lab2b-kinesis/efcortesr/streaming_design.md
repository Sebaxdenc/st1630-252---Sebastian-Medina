# Diseño de streaming - Lab 2b

**Curso:** ST1630-2026-2 - **Semana:** S7 - **Fecha:** 2026-09-13  
**Estudiantes:** Emmanuel Cortes, Sara Hurtado, Juan Jose Osorio y Mariana Sanchez

## Evidencia de la ejecucion

- Fuente: `pedidos-ventas` en Kafka, con 4 particiones.
- Tres publicaciones: 3.000 pedidos confirmados por el productor.
- Resultado Delta: 12 filas, suma de `num_pedidos` igual a 3.000.
- Las dos publicaciones de la ventana actual conservaron las mismas 6
  claves y actualizaron sus agregados mediante `MERGE`.
- Claves duplicadas por `(window_start, window_end, region)`: 0.
- Trigger configurado: 30 segundos.
- Ventana implementada: 5 minutos.
- Watermark implementado: 10 minutos.
- Ruta de checkpoint: `.lab2b/lake/checkpoints/lab2b-kafka`.

## Pregunta 1 - Ventana y watermark

¿Por que una ventana de 5 minutos y un watermark de 10 minutos son
adecuados para este productor? Explica tambien que ocurre con un evento
que supera el watermark respecto al evento mas reciente observado.

**Respuesta:** Elegimos ventanas fijas de 5 minutos y un watermark de
10 minutos. El productor genera 1.000 pedidos en pocos segundos, por lo
que una ventana de 5 minutos permite observar resultados frecuentes y
tambien ejecutar varias veces el productor sobre una ventana que sigue
abierta. El watermark de 10 minutos tolera eventos retrasados durante el
equivalente a dos ventanas, sin obligar a Spark a conservar el estado de
las ventanas indefinidamente.

Spark calcula el watermark a partir del mayor `kafka_time` observado. Si
un pedido llega con un tiempo de evento anterior al limite marcado por el
watermark y la ventana correspondiente ya fue cerrada, Spark lo descarta
de esta agregacion con estado. El mensaje puede haber sido leido desde
Kafka, pero deja de modificar el resultado de esa ventana.

## Pregunta 2 - Checkpoint vs. commit manual

¿Quien recuerda el progreso en Structured Streaming? Compara el
`checkpointLocation` con el `consumer.commit()` manual del Lab 2a.

**Respuesta:** Spark Structured Streaming es responsable de recordar el
progreso. En este pipeline guarda en `checkpointLocation` los offsets de
Kafka procesados, los metadatos de cada micro-batch y el estado de las
agregaciones por ventana.

Se parece al `consumer.commit()` del Lab 2a porque ambos mecanismos
registran hasta que posicion de Kafka se proceso y permiten continuar
despues de un reinicio. La diferencia es que en el Lab 2a nuestro codigo
decidia explicitamente cuando ejecutar `commit()` despues del `MERGE`.
Aqui Spark coordina automáticamente el checkpoint con la ejecucion de
cada micro-batch y tambien conserva el estado necesario para las
ventanas y el watermark, no solamente los offsets del consumidor.

## Pregunta 3 - outputMode

Justifica `outputMode("update")` y explica que ocurriria con `complete`
y `append` para esta agregacion con ventanas.

**Respuesta:** Usamos `update` porque cada micro-batch entrega solamente
las filas agregadas que cambiaron. Una ventana puede recibir pedidos en
varios batches antes de cerrarse, y `foreachBatch` puede aplicar esas
filas al destino mediante el `MERGE`.

Con `complete`, Spark enviaria la tabla completa de agregados en cada
trigger, incluyendo ventanas y regiones que no cambiaron. El resultado
podria mantenerse correcto con un `MERGE`, pero aumentarian la lectura,
el shuffle y las escrituras sin necesidad. Con `append`, Spark solo
podria emitir una ventana cuando el watermark la considere terminada;
no mostraria las actualizaciones mientras la ventana esta abierta. Un
append directo al destino, ademas, produciria varias versiones de la
misma ventana y region si no se aplicara el `MERGE`.

## Pregunta 4 - La llave del MERGE

Explica por que la llave cambio de `pedido_id` a
`(window_start, window_end, region)` y por que el resultado sigue siendo
idempotente.

**Respuesta:** En el Lab 2a cada fila representaba un pedido, por eso
`pedido_id` identificaba naturalmente el registro. En este pipeline cada
fila representa el agregado de todos los pedidos de una region durante
una ventana. Despues del `groupBy`, `pedido_id` ya no identifica la fila
resultante ni forma parte de su esquema.

La combinacion `(window_start, window_end, region)` identifica de manera
unica cada agregado. Si llegan mas pedidos para la misma region y
ventana, `whenMatchedUpdateAll()` reemplaza los totales anteriores por
los nuevos. Si la clave aun no existe, `whenNotMatchedInsertAll()` crea
la fila. Esto hace que reintentar el mismo resultado sea idempotente:
actualiza la misma clave en lugar de insertar un duplicado. En la prueba,
dos publicaciones dentro de la misma ventana conservaron seis claves y
el resultado final tuvo cero claves duplicadas.

## Pregunta 5 - Trigger interval

Analiza el trigger de 30 segundos frente a la ventana de 5 minutos y el
trade-off entre latencia y costo de micro-batches mas frecuentes.

**Respuesta:** El pipeline usa `processingTime="30 seconds"`. Como la
ventana dura 5 minutos, puede producir hasta diez actualizaciones antes
de que termine una ventana, permitiendo observar cambios con una demora
razonable para este laboratorio.

Un intervalo mas corto reduce la latencia, pero crea mas micro-batches,
mas operaciones de checkpoint y mas ejecuciones del `MERGE`, incluso
cuando llegan pocos datos. Un intervalo mas largo agrupa mas registros y
reduce ese costo, pero retrasa la visualizacion de los resultados. En la
prueba local algunos batches tardaron mas de 30 segundos por el costo de
Spark y Delta sobre WSL; para produccion el intervalo debe ajustarse al
volumen, la capacidad del cluster y la latencia requerida.

## Pregunta 6 - Reinicio del job

Explica que ocurre al detener y reiniciar el pipeline con el mismo
checkpoint y por que el sink Delta protege el resultado si un batch se
reintenta.

**Respuesta:** Con el mismo checkpoint, Spark consulta los offsets ya
confirmados y continua desde el siguiente dato disponible; normalmente
no vuelve a leer todo el topic. Durante una falla puede reintentar un
micro-batch cuya finalizacion no quedo registrada, porque el checkpoint y
un sistema externo como Delta no forman una sola transaccion distribuida.

Ese reintento es seguro para este sink porque `escribir_batch()` no hace
append. El `MERGE` busca la clave `(window_start, window_end, region)` y
actualiza la fila si ya existe. Por eso procesar otra vez el mismo
agregado deja una sola fila con sus valores actuales. Si se elimina el
checkpoint, Spark vuelve a leer desde `earliest`; el mismo `MERGE` evita
duplicar las claves, aunque se incurre nuevamente en el costo de procesar
los datos.

## Pregunta 7 - Kinesis

No realizada. El rol temporal de AWS Academy permitio autenticar y listar
streams, pero respondio `AccessDeniedException` para
`kinesis:CreateStream`. No se creo ningun recurso Kinesis.
