# CuprumAndes SpA (rubro: mineria del cobre)

## Contexto del rubro
El cobre es la viga maestra de la economia chilena: Chile produce cerca de un cuarto del cobre del mundo y la
mineria explica una porcion enorme de las exportaciones del pais. Es un negocio de escala gigante y de margenes
que dependen de dos cosas que la empresa no controla del todo: la ley del yacimiento (cuanto cobre hay en cada
tonelada de roca) y el precio internacional del metal. Como el precio se fija afuera y en dolares, una faena no
compite subiendo el precio, compite bajando su costo por libra y exprimiendo cada punto de ley y de recuperacion.

Tres rasgos definen al sector y explican por que los datos importan tanto:
- La ley cae con los anios. A medida que se agota un yacimiento, la roca que queda tiene menos cobre por
  tonelada. Procesar la misma cantidad de material entrega menos metal: es el drama estructural de la mineria
  madura, y se ve en los datos como una caida del cobre fino aunque el tonelaje no baje.
- La disponibilidad de los equipos lo es todo. Una planta detenida no procesa, y cada hora de detencion es
  tonelaje que no se mueve y metal que no se produce. Saber donde y por que se detiene la operacion (que planta,
  que causa) es plata directa.
- El costo se mide en dolares por libra. El famoso cash cost permite comparar faenas y saber si a la empresa le
  fue bien porque subio el precio del cobre o porque de verdad produjo mas barato. Son cosas distintas y es facil
  confundirlas.

La pregunta de fondo ya no es "cuanto cobre movimos", sino "estamos produciendo mas barato y aprovechando la ley,
o el buen resultado del anio es solo el precio que nos regalo el mercado".

## La empresa
CuprumAndes SpA es una minera de cobre de tamano mediano, con operaciones en el norte y centro del pais. Opera
cuatro plantas concentradoras que reciben mineral de cinco rajos (minas a cielo abierto), lo procesan por turnos
las 24 horas y producen concentrado de cobre. Su negocio se juega en el detalle operacional: cuanta roca trata
cada dia, con que ley viene, cuanto recupera la planta, cuantas horas para y cuanto le cuesta cada libra.

Durante 2025 la gerencia de operaciones nota algo que la incomoda: el precio del cobre estuvo bueno y las ventas
acompanaron, pero la sensacion en planta es que se esta produciendo con mas esfuerzo y mas costo que antes. El
directorio quiere separar el efecto del precio (que es suerte de mercado) del desempeno propio (ley, detenciones,
costo por libra). Nadie ha cruzado todavia las cuatro fuentes con esa lente. Ahi entran ustedes, un equipo
externo con acceso a los datos de la faena para responder con evidencia.

## Conceptos clave del rubro
- Ley del mineral: cuanto cobre hay por tonelada de roca, en porcentaje. Una ley de 1% quiere decir 10 kilos de
  cobre por tonelada. Es la materia prima de todo el negocio.
- Recuperacion metalurgica: de todo el cobre que entra a la planta, que porcentaje logra recuperarse en el
  concentrado. El resto se pierde en el proceso.
- Cobre fino vs bruto: el fino es el cobre puro contenido, y se calcula como toneladas tratadas por ley por
  recuperacion. Es la medida que de verdad importa, no las toneladas de roca (bruto).
- Ley de corte: la ley minima que hace rentable procesar una roca. Bajo esa ley, tratar el material cuesta mas de
  lo que rinde.
- Rajo: la mina a cielo abierto de donde sale el mineral. Distintos rajos tienen distinta ley.
- Tratamiento: el tonelaje de mineral que la planta procesa en un periodo.
- Disponibilidad y utilizacion: que porcentaje del tiempo el equipo esta disponible para operar y que porcentaje
  efectivamente opera. Las detenciones las bajan.
- Detencion: una parada de la operacion (mantencion, falla, falta de camiones, clima). Se mide en horas y en
  tonelaje no movido.
- Lote o campana: un bloque de mineral procesado, el grano mas fino con que se registra la operacion.
- Cash cost (C1): el costo directo de producir una libra de cobre, en dolares. Permite comparar faenas y saber si
  la empresa produce caro o barato, mas alla del precio de mercado.

## Preguntas que la gerencia busca entender
Son las mismas para los tres equipos que trabajan este caso:
1. El cobre fino que producimos, esta cayendo a lo largo del anio? Y si cae, es porque tratamos menos roca o
   porque baja la ley de nuestro mineral?
2. Donde se concentran las horas de detencion (que planta, que causas) y cuanto nos cuestan en tonelaje que no se
   alcanza a mover?
3. El costo de producir una libra de cobre (cash cost), subio o bajo durante el anio? Como se compara con lo que
   hizo el precio del cobre: nos fue bien por precio o de verdad producimos mas barato?
4. La ley que reporta operaciones y la que reporta el laboratorio, cuadran entre si? Con cual conviene calcular el
   cobre fino?

## Fuentes disponibles
La empresa entrega cuatro fuentes:
- `origen_mina.sqlite`: la base operacional, con varias tablas relacionadas: lotes procesados, detenciones,
  plantas, rajos, equipos, turnos, causas de detencion y regiones.
- `origen_costos_planta.csv`: los costos de cada planta por mes (energia, insumos, mano de obra), del area de
  Control de Gestion.
- `origen_leyes_laboratorio.json`: los resultados de laboratorio de las leyes de las muestras.
- La API del cobre y del dolar: el precio de la libra de cobre y el dolar observado, desde
  `https://mindicador.cl/api/libra_cobre/2025` y `https://mindicador.cl/api/dolar/2025`.
