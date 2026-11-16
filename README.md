# biblioteca-eventos

| Qué falla | Qué hace el consumidor | Por qué |
|---|---|---|
| JSON roto, falta `x-evento-id` o un campo obligatorio, o Postgres rechaza el dato (códigos `22...` y `23...`) | `nack(mensaje, false, false)` de inmediato | Reintentar no lo arregla nunca |
| El `insert` choca con `23505` | `ack` | Ya estaba guardado: es la reentrega del 2.3 |
| Cualquier otro error de la base (`ECONNREFUSED`, conexión cortada) | Hasta tres intentos, esperando 2 s y después 4 s; si se agotan, `nack(mensaje, false, false)` | El mensaje está bien; el que está mal es el entorno |

## Kafka (L10): cuatro consumer groups y el panel en vivo

`src/streaming/` es aparte de los consumidores de RabbitMQ de arriba. En L9 había un solo grupo que solo registraba;
desde L10 hay **cuatro**, cada uno en su carpeta y su módulo de Nest, que escriben en Postgres y confirman el offset
**a mano, después de escribir**. Variable: `KAFKA_BROKERS` (en el equipo, `localhost:9092,localhost:9094,localhost:9096`;
en el compose, `broker-1:19092,broker-2:19092,broker-3:19092`).

| Consumer group | Lee | Escribe |
|---|---|---|
| `biblioteca-panel` | `biblioteca.lecturas` y `biblioteca.prestamos` | `kpi_minuto`: una fila por `lectura.iniciada`, `lectura.terminada` y `prestamo.creado` |
| `biblioteca-alertas` | `biblioteca.lecturas` | `ventanas_lectura` (por libro y ventana de 5 minutos) y `alertas` |
| `biblioteca-historial` | `biblioteca.prestamos` | `prestamos_semana` (por semana y libro) y `prestamos_vistos` |
| `biblioteca-catalogo` | `biblioteca.catalogo` (compactado), siempre desde el offset 0 | `catalogo_libros` |

| Archivo | Qué es |
|---|---|
| `src/streaming/consumidor-kafka.ts` | **La clase de Kafka** que heredan los cuatro: conexión, suscripción (`autocommit: false`), el ciclo leer, validar, procesar y `commit`, el resumen cada 10 s en el log y `close()` en `onModuleDestroy` |
| `src/streaming/contrato.ts` | `Contrato.validar(m)`: `JSON.parse` y el DTO con `class-validator`; si falla, lanza `EventoInvalido` con el motivo |
| `src/streaming/evento.dto.ts` | El sobre y los `datos` de los cinco tipos de lecturas y préstamos |
| `src/streaming/ventanas.ts` | El minuto y la ventana de 5 minutos de `emitidoEn` (UTC) |
| `src/streaming/evento-procesado.entity.ts` | `eventos_procesados (grupo, evento_id)`: la idempotencia de alertas e historial |
| `src/streaming/<grupo>/<grupo>.consumidor.ts` | El grupo: su nombre, sus topics y su clase de negocio (una línea de configuración sobre `ConsumidorKafka`) |
| `src/streaming/<grupo>/<grupo>.negocio.ts` | **La clase de negocio**: recibe el evento validado y escribe en Postgres; devuelve `nuevo`, `duplicado` o `ignorado`. No importa nada de Kafka |
| `src/streaming/<grupo>/*.entity.ts` y `<grupo>.module.ts` | Sus tablas (TypeORM, `synchronize`) y su módulo |
| `src/streaming/panel/panel-en-vivo.controller.ts` y `.service.ts` | `GET /panel/en-vivo`: lo que lee el BFF para el panel |
| `src/streaming/catalogo/evento-catalogo.dto.ts` | El sobre de `libro.publicado` y `libro.actualizado` |

| Qué llega | Qué hace | Por qué |
|---|---|---|
| Un evento válido y nuevo | Escribe y **después** confirma | Si muere entre los dos, lo relee y el `INSERT` no suma (idempotencia) |
| Un duplicado (mismo `eventoId`) | No suma, lo registra como `DUPLICADO` y confirma | `ON CONFLICT DO NOTHING` sobre `evento_id`, o sobre `(grupo, evento_id)` |
| Un tipo que el grupo no usa | Lo cuenta como `ignorado` y confirma | Por ejemplo, `capitulo.terminado` en el panel |
| No es JSON, o no cumple el DTO | Lo registra como `INVALIDO` con el motivo y **confirma** | Reintentar no lo arregla; en la E3 va al `.dlt` |
| Postgres no responde | Reintenta el mismo evento cada 2 s y **no** confirma | Confirmarlo lo perdería |

`GET /panel/en-vivo` (puerto 3010) devuelve, en hora de Chile: los totales (lecturas iniciadas, terminadas y en
curso, préstamos), el máximo de lecturas en curso, la serie por minuto, los libros (con el título de
`catalogo_libros`) y las alertas vigentes. No pide token: solo lo llama el BFF por la red interna, y el rol lo revisa
el BFF. Con la noche del simulador da 452 lecturas en curso a las 22:00, un máximo de 537 a las 21:48, 380 préstamos
y la alerta del libro `10` de 21:30 a 21:35 (56 de 59 en error).

Cada 10 s, si cambió algo, cada grupo deja una línea así en el log:

```
LOG [biblioteca-panel] desde el arranque: 2626 nuevos, 15 duplicados, 714 ignorados, 33 invalidos
```

Para ver el avance, desde `biblioteca-plataforma`: `docker compose exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh
--bootstrap-server broker-1:19092 --describe --group biblioteca-panel` (y los otros tres), con `LAG` en 0 cuando
termina de leer. El rebobinado de `biblioteca-historial`, la limpieza para repetir y las instancias extra están en el
`README.md` de `biblioteca-plataforma`; las decisiones (idempotencia, mínimo de la alerta), en su
`docs/consumer-groups.md`.

Si cae el clúster entero, el consumidor se detiene (`el consumidor se detuvo` en el log) y hay que reiniciar
`eventos`; con uno o dos brokers caídos sigue leyendo.
