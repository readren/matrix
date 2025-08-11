package readren.sequencer.akka

import akka.actor.{Actor, ActorRef, Timers}
import akka.pattern.ask

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Herramienta que facilita el uso de [[Future]]s en un [[Actor]]; y hace que sea mas claro de entender el orden en que se ejecutan los sectores de código cuando existen sectores que no son ejecutados por este actor.
 * Usando esta herramienta el código luce mas declarativo, y se reduce  (o incluso anula) la cantidad de variables de estado del [[Actor]].
 * Para que esta herramienta funcione se requiere que el método `efectuarTareaProgramada` sea parte del comportamiento del actor.
 *
 * @define cuandoCompletaEjecutadaPorEsteActor El call-back `cuandoCompleta` pasado a `efectuar/ejecutar` es siempre, sin excepción, ejecutado por este actor sin envoltura `try..catch`. Esto es parte del contrato de este trait.
 * @define desdeCualquierHilo                  Este y todos los métodos de este trait pueden ser invocados desde cualquier hilo de ejecución.
 * @define ejecutadaPorEsteActor               Es importante resaltar que esta función es ejecutada por este [[Actor]]. Por ende, puede leer y modificar las variables de este actor.
 * @define estaCuidada                         Si esta función terminara abruptamente, la tarea dada terminará con la falla lanzada.
 * @define workaroundAbstractMethodError       WORKAROUND: Esta clase debería ser privada, pero se hizo pública para eludir un bug del compilador incremental (del scala-IDE) que provoca que cada vez que se toca el código y actúa el compilador incremental (no ocurre si se hace un clean) se corrompa el build y la excepción "java.lang.AbstractMethodError" sea lanzada cuando se intenta crear una instancia de la clase.
 */
trait AmigoFutures { actor: Actor =>

	case class Efectuar(accion: () => Unit);

	private val debug: Boolean = actor.context.system.settings.config.getBoolean("amigoFutures.debug");
	private var hiloActualActor: Thread = Thread.currentThread;

	object ejecutadoPorEsteActor extends ExecutionContext {
		def execute(runnable: Runnable): Unit =
			actor.self ! Efectuar { () => runnable.run() }

		def reportFailure(cause: Throwable): Unit =
			actor.context.system.log.error(cause, "Reporte de computanción diferida fallida en actor {} : ", actor.self);
	}

	/** Debe ser parte del comportamiento del actor para que los miembros de este trait funcionen.
	 * Uso: {{{
	 * class UnTipoDeActor extends AmigoFutures {
	 *   def receive: Receive =
	 *     this.efectuarTareaProgramada orElse {
	 *       case someMatching => someAction
	 *       ...
	 *     }
	 * }
	 * }}}
	 */
	protected val efectuarTareaProgramada: akka.actor.Actor.Receive = new PartialFunction[Any, Unit] {
		def isDefinedAt(x: Any): Boolean = {
			if (debug)
				hiloActualActor = Thread.currentThread();
			x.isInstanceOf[Efectuar]
		}

		def apply(x: Any): Unit = x.asInstanceOf[Efectuar].accion();
	};

	/** Programa la ejecución, por parte de este [[Actor]], de la `acción` recibida.
	 * Esta función solo tiene sentido llamarla desde una acción que no es ejecutada por este actor. El call-back de un [[Future]] por ejemplo.
	 *
	 * =Nota:=
	 * Es equivalente a: {{{ Tarea.mia(accion).efectuarYDesechar(()); }}}
	 */
	final def ejecutarYoMismo(accion: () => Unit): Unit = {
		actor.self ! Efectuar(accion)
	}

	////////////////////////////////

	object Tarea {
		/** Crea una tarea que al ejecutarla programa la llamada de `cuandoCompleta(Success(a))`.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 */
		def trivial[A](a: A): Tarea[A] = new TareaTrivial(Success(a));

		/** Crea una tarea que al ejecutarla programa la llamada de `cuandoCompleta(Failure(falla))`.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 */
		def fallida[A](falla: Throwable): Tarea[A] = new TareaTrivial(Failure(falla));

		/** Crea una tarea que al ejecutarla programa la realización por parte de este actor de la `acción` recibida.
		 * Y luego, después que la `acción` haya sido efectuada, llama a `cuandoCompleta` pasándole:
		 * - el resultado si `acción` termina normalmente;
		 * - o `Failure(excepción)` si la `acción` termina abruptamente.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 *
		 * @param accion La acción que estará encapsulada en la tarea creada. $ejecutadaPorEsteActor
		 */
		def propia[A](accion: () => Try[A]): Tarea[A] = new TareaPropia(accion);

		/** Crea una tarea que al ejecutarla programa la realización por parte de este actor de la `acción` recibida.
		 * Y luego, después que la `acción` haya sido efectuada, llama a `cuandoCompleta` pasándole:
		 * - `Success(resultado)` si la `acción` termina normalmente;
		 * - o `Failure(excepción)` si la `acción` termina abruptamente.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * Equivale a {{{ propia { () => Success(accion()) } }}}
		 *
		 * @param accion La acción que estará encapsulada en la tarea creada. $ejecutadaPorEsteActor
		 *
		 */
		def mia[A](accion: () => A): Tarea[A] = new TareaPropia(() => Success(accion()));

		/** Crea una tarea que espera que el [[Future]] recibido haya completado, y da el resultado de dicho [[Future]].
		 * Ejecutar la [[Tarea]] dada provoca que el call-back `cuandoCompleta` sea llamado cuando el `future` complete pasándole el resultado del mismo.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 *
		 * @param f que tendrá el resultado de la tarea.
		 */
		def ajena[A](f: Future[A]): Tarea[A] = new TareaAjena(f);

		/** Crea una tarea que al ejecutarla:
		 *  - primero ejecuta las `tareaA` y `tareaB` en simultaneo (en oposición a esperar que termine una para ejecutar la otra)
		 *  - segundo, cuando ambas terminan, sea exitosamente o fallidamente, se aplica la función `f` al resultado de las dos tareas.
		 *  - ultimo llama a `cuandooCompleta`con:
		 *  	- la excepción lanzada por la función `f` si termina abruptamente;
		 *  	- o el resultado de f si termina normalmente
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 *
		 * $workaroundAbstractMethodError
		 */
		def bifurca[A, B, C](tareaA: Tarea[A], tareaB: Tarea[B])(f: (Try[A], Try[B]) => Try[C]): Tarea[C] =
			new TareaBifurcada(tareaA, tareaB, f)

		/** Crea una tarea que al ejecutarla ejecuta las tareas recibidas en paralelo, y da como resultado una lista con sus resultados. */
		def sequence[A](tareas: List[Tarea[A]]): Tarea[List[A]] = {
			@tailrec
			def loop(acum1: Tarea[List[A]], restantes: List[Tarea[A]]): Tarea[List[A]] = {
				restantes match {
					case Nil =>
						acum1
					case anteultima :: anteriores =>
						val acum2 = bifurca(acum1, anteultima) { (tla, ta) =>
							for {
								la <- tla
								a <- ta
							} yield a :: la
						}
						loop(acum2, anteriores)
				}
			}

			tareas.reverse match {
				case Nil => trivial(Nil)
				case ultima :: anteriores => loop(ultima.map(List(_)), anteriores);
			}
		}

		/** Crea una [[Tarea]], llamémosla "bucle", que al ejecutarla hace lo siguiente:
		 *  - ejecuta la función `condición(a0)` recibida y si dicha ejecución:
		 *  	- termina abruptamente, la tarea bucle completa con `Failure(excepción)`
		 *  	- termina normalmente con:
		 *  		- `Left(tryB)`, la tarea bucle completa con `tryB`
		 *  		- `Right(tareaA)`, ejecuta la `tareaA` y si:
		 *  			- completa con falla, la tarea bucle completa con esa falla.
		 *  			- completa exitosamente con el valor `a1`, se vuelve al primer paso pero reemplazando `a0` con `a1`.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 *
		 * @param condicion función que determina si habrá iteración y, de haberla, da la tarea que sería ejecutada. $ejecutadaPorEsteActor
		 */
		def mientrasCondicionRepite[A, B](a0: A)(condicion: A => Either[Try[B], Tarea[A]]): Tarea[B] =
			TareaBucle[A, B](a0)(condicion);

		/** Crea una [[Tarea]], llamémosla "bucle", que al ejecutarla hace lo siguiente:
		 *  - ejecuta función `condición(a0)` recibida y si dicha ejecución:
		 *  	- termina abruptamente, la tarea bucle completa con `Failure(excepción)`
		 *  	- termina normalmente, ejecuta la tarea resultante y si el resultado de dicha tarea es:
		 *  		- `Left(tryB)`, la tarea bucle completa con `tryB`
		 *  		- `Right(a1)`, se vuelve a primer paso pero reemplazando `a0` con `a1`.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 *
		 * @param condicion función que crea la tarea que se ejecutará en la iteración. El resultado de dicha tarea determina si habrá otra iteración. $ejecutadaPorEsteActor
		 */
		def repiteHastaCondicion[A, B](a0: A)(condicion: A => Tarea[Either[Try[B], A]]): Tarea[B] =
			TareaBucle[Either[Try[B], A], B](Right(a0)) {
				_ map condicion
			}

		/** Crea una [[Tarea]], llamémosla "bucle" que ejecuta la `tarea` recibida y si el resultado es instancia de [[Left]] la vuelve ejecutar repetidamente hasta que una de tres: el resultado sea instancia de [[Right]], se acaben los reintentos, o falle.
		 * El resultado de la tarea dada es el resultado que dio la `tarea` recibida en la última iteración, o `Failure(excepción)` si termina abruptamente.
		 * La `tarea` recibida siempre es ejecutada al menos una vez, incluso si la `cantReintentos` es menor a cero.
		 */
		def reintenta[A, B](cantReintentos: Int)(tarea: Tarea[Either[A, B]]): Tarea[Either[A, B]] =
			new Tarea[Either[A, B]] {
				override def realizar(llameYo: Boolean)(cuandoCompleta: Try[Either[A, B]] => Unit): Unit = {
					def trabajo(): Unit = {
						tarea.realizar(llameYo = true) {

							case Success(Right(exito)) =>
								cuandoCompleta(Success(Right(exito)));

							case Success(Left(rechazo)) =>
								if (cantReintentos > 0) {
									reintenta(cantReintentos - 1)(tarea).realizar(llameYo = true)(cuandoCompleta);
								} else {
									cuandoCompleta(Success(Left(rechazo)));
								}

							case Failure(causa) =>
								cuandoCompleta(Failure(causa));
						}
					}

					if (llameYo) trabajo();
					else actor.self ! Efectuar(() => trabajo());
				}
			}

		def pregunta[Respuesta: ClassTag](destino: ActorRef, mensaje: Any, timeout: FiniteDuration): Tarea[Respuesta] =
			ajena(ask(destino, mensaje, actor.self)(timeout).mapTo[Respuesta])
	}

	/** Encapsula una acción aportando mecanismos para facilitar la ejecución y obtención del resultado desde este actor.
	 * Notar que [[Tarea]] es un monad.
	 *
	 * @tparam A tipo del resultado de la acción encapsulada.
	 *
	 *           Nota: Es necesario que este trait sea interno para que el compilador chille si una instancia es usada fuera del actor.
	 */
	trait Tarea[+A] { tarea =>

		/** Inicia la ejecución de la acción encapsulada por esta tarea.
		 * La operación `cuandoCompleta` recibida será ejecutada por este actor cuando la ejecución de la acción encapsulada haya termine, sea normal o abruptamente.
		 * El argumento `llameYo` puede ser `true` solo si se tiene la certeza que esta operación es invocada por este actor. De lo contrario debe ser `false`. Usar `false` cuando se tiene alguna duda.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 */
		protected[AmigoFutures] def realizar(llameYo: Boolean)(cuandoCompleta: Try[A] => Unit): Unit;

		def efectuar(llameYo: Boolean)(cuandoCompleta: Try[A] => Unit): Unit = {
			if (llameYo && debug && (Thread.currentThread() ne hiloActualActor)) {
				throw new AssertionError(s"currentThread=${Thread.currentThread.getName}, hiloActualActor=${hiloActualActor.getName}");
			}
			this.realizar(llameYo)(cuandoCompleta)
		}

		/** Inicia la ejecución de la acción encapsulada por esta tarea.
		 * La operación `cuandoCompleta` recibida será ejecutada por este actor cuando la ejecución de la acción encapsulada haya termine, sea normal o abruptamente.
		 * Puede ser llamado desde un hilo de ejecución distinto al de este actor.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 */
		@inline
		def ejecutar(cuandoCompleta: Try[A] => Unit): Unit = this.realizar(llameYo = false)(cuandoCompleta)

		/** Inicia la ejecución de la acción encapsulada.
		 * El future dado será completado cuando la acción encapsulada termine.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 */
		def correr(llameYo: Boolean): Future[A] = {
			val p = Promise[A]()
			efectuar(llameYo) {
				p.complete
			}
			p.future
		}

		/** Ejecuta esta [[Tarea]] ignorando tanto el resultado como si terminó normal o abruptamente.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 */
		def efectuarYDesechar(llameYo: Boolean): Unit =
			this.efectuar(llameYo)(_ => {})

		/** Ejecuta esta [[Tarea]] y si termina abruptamente llama a la función recibida.
		 * La función `manejadorError` recibida será ejecutada por este actor si la acción encapsulada termina abruptamente.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 */
		def efectuarYManejar(llameYo: Boolean)(manejadorError: Throwable => Unit): Unit =
			this.realizar(llameYo) {
				case Failure(NonFatal(e)) => manejadorError(e);
				case _ =>
			}

		/** $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 */
		def efectuarYEnviar(llameYo: Boolean, destino: ActorRef)(f: Throwable => Throwable = identity): Unit = {
			this.efectuar(llameYo) {
				case Success(r) => destino ! r;
				case Failure(e) => destino ! akka.actor.Status.Failure(f(e))
			}
		}

		/** Da una [[Tarea]] que al ejecutarla:
		 *  - primero ejecuta ésta tarea,
		 *  - segundo aplica la función `f` al resultado
		 * 	- último llama a `cuandoCompleta` con:
		 *  	- la excepción lanzada en el segundo paso por la operación `ctorTarea` si termina abruptamente,
		 *  	- o el resultado de la aplicación del segundo paso.
		 *      Análogo a Future.transform.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 *
		 * @param f función que se aplica al resultado de esta [[Tarea]] para determinar el resultado de la tarea dada. $ejecutadaPorEsteActor $estaCuidada
		 */
		@inline
		def transform[B](f: Try[A] => Try[B]): Tarea[B] =
			new TareaTransformada(tarea, f)

		/** Da una [[Tarea]] que al ejecutarla:
		 *  - primero ejecuta esta tarea;
		 *  - segundo aplica la función `ctorTarea` al resultado, y si `ctorTarea` termina normalmente:
		 *  	- tercero ejecuta la tarea creada;
		 *  - último llama a `cuandoCompleta` con:
		 *  	- la excepción lanzada en el segundo paso por la operación `ctorTarea` si termina abruptamente;
		 *  	- o el resultado de la ejecución del tercer paso.
		 *      Análogo a Future.transformWith.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 *
		 * @param ctorTarea función que se aplica al resultado de esta [[Tarea]] para crear la [[Tarea]] que se ejecutará después. $ejecutadaPorEsteActor $estaCuidada
		 */
		@inline
		def transformWith[B](ctorTarea: Try[A] => Tarea[B]): Tarea[B] =
			new TareaCompuesta(tarea, ctorTarea)

		/** Da una [[Tarea]] que primero hace ésta tarea y luego, si el resultado es exitoso, aplica la función `f` al resultado.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 *
		 * @param f función que transforma el resultado de la tarea. $ejecutadaPorEsteActor $estaCuidada
		 */
		def map[B](f: A => B): Tarea[B] = transform(_ map f)

		/** Da una [[Tarea]] que al ejecutarla:
		 *  - primero ejecuta esta [[Tarea]], y si el resultado es exitoso:
		 *  	- segundo aplica la función `ctorTarea` al resultado, y si `ctorTarea` termina normalmente:
		 *  		- tercero ejecuta la tarea creada en el paso anterior.
		 *  - último llama a `cuandoCompleta` con:
		 *  	- el resultado de la ejecución del primer paso si es fallido;
		 *  	- la excepción lanzada en el segundo paso por la operación `ctorTarea` si termina abruptamente;
		 *  	- o el resultado de la ejecución del tercer paso.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 *
		 * @param ctorTarea función que se aplica al resultado del primer paso para crear la [[Tarea]] que se ejecutaría luego de ésta como parte de la tarea dada. $ejecutadaPorEsteActor $estaCuidada
		 */
		def flatMap[B](ctorTarea: A => Tarea[B]): Tarea[B] = transformWith {
			case Success(s) => ctorTarea(s);
			case Failure(e) => new TareaTrivial(Failure(e)); //this.asInstanceOf[Tarea[B]]
		}

		def withFilter(p: A => Boolean): Tarea[A] = new Tarea[A] {
			def realizar(llameYo: Boolean)(cuandoCompleta: Try[A] => Unit): Unit = {
				def trabajo(): Unit = tarea.realizar(llameYo = true) {
					case s@Success(a) =>
						if (p(a))
							cuandoCompleta(s)
						else
							cuandoCompleta(Failure(new NoSuchElementException(s"Tarea filter predicate is not satisfied for $a")))

					case f@Failure(_) =>
						cuandoCompleta(f)
				}

				if (llameYo) trabajo();
				else actor.self ! Efectuar(() => trabajo())
			}
		}

		/** Encadena un efecto secundario.
		 *
		 * El resultado de la tarea dada es el mismo que daría esta tarea, ignorando como termine la ejecución de `p` y su resultado.
		 * Para quien no ve el efecto secundario la diferencia entre esta tarea y la dada es solo el tiempo que consume la ejecución.
		 *
		 * Específicamente hablando, da una [[Tarea]] que al ejecutarla:
		 * 	- primero ejecuta esta tarea y, si el resultado es exitoso:
		 * 		- segundo aplica la función `p` al resultado de esta [[Tarea]].
		 * 	- ultimo llama a `cuandoCompleta` con:
		 * 		- el resultado de la ejecución del primer paso sea exitoso o fallido;
		 *          Es equivalente a:
		 * {{{
		 * 		transform[A] { ta =>
		 * 			try p(ta)
		 * 		catch { case NonFatal(e) => log.error(e, "..") }
		 * 	}
		 * }}}
		 *          $cuandoCompletaEjecutadaPorEsteActor
		 *          $desdeCualquierHilo
		 *
		 * @param p función parcial que representa el efecto secundario. El resultado de esta función parcial es ignorado.  $ejecutadaPorEsteActor
		 */
		def andThen[U](p: Try[A] => U): Tarea[A] = {
			transform {
				ta =>
					try p(ta)
					catch {
						case NonFatal(e) => actor.context.system.log.error(e, "La ejecución de un procedimiento secundario ha fallado:")
					}
					ta
			}
		}

		/** Como `map` pero para las fallas.
		 * Da una [[Tarea]] que al ejecutarla:
		 *  - primero ejecuta esta tarea y, si el resultado es fallido y la función parcial `pf` esta definida para dicha falla:
		 *  	- segundo aplica la función parcial `pf` a la falla;
		 *  - último llama a `cuandoCompleta` con:
		 *  	- el resultado del primero paso si es exitoso, o fallido donde la función parcial `pf` no este definida;
		 *  	- la excepción lanzada en el segundo paso por la función `pf` si termina abruptamente;
		 *  	- o el resultado del segundo paso.
		 *      Análogo a Future.recover.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 *
		 * @param pf función parcial que se aplica al resultado del primer paso si es fallido para determinar el resultado de la tarea dada. $ejecutadaPorEsteActor $estaCuidada
		 */
		def recover[B >: A](pf: PartialFunction[Throwable, B]): Tarea[B] =
			transform {
				_.recover(pf)
			}

		/** Como `flatMap` pero para las fallas
		 * Da una [[Tarea]] que al ejecutarla:
		 *  - primero ejecuta esta tarea, y si el resultado es fallido y la función parcial `pf` esta definida para dicha falla:
		 *  	- segundo aplica la función parcial `pf` a la falla, y si `pf` termina normalmente:
		 *  		- tercero ejecuta la tarea creada en el paso anterior;
		 *  - último llama a `cuandoCompleta` con:
		 *  	- el resultado del primero paso si es exitoso o fallido donde la función parcial `pf` no este definida;
		 *  	- la excepción lanzada en el segundo paso por la función parcial `pf` si termina abruptamente;
		 *  	- o el resultado del tercer paso.
		 *      Análogo a Future.recoverWith.
		 *
		 * $cuandoCompletaEjecutadaPorEsteActor
		 * $desdeCualquierHilo
		 *
		 * @param pf función parcial que se aplica al resultado del primer paso si es fallido para crear la [[Tarea]] que se ejecutaría luego de ésta como parte de la tarea dada. $ejecutadaPorEsteActor $estaCuidada
		 */
		def recoverWith[B >: A](pf: PartialFunction[Throwable, Tarea[B]]): Tarea[B] = {
			transformWith[B] {
				case Failure(t) => pf.applyOrElse(t, (e: Throwable) => new TareaTrivial(Failure(e)));
				case sa@Success(_) => new TareaTrivial(sa);
			}
		}

		/** Da una nueva [[Tarea]] que repite esta [[Tarea]] hasta que esté definido el [[Option]] resultante  de aplicar la función recibida en la dupla formada por el resultado que de esta [[Tarea]] y la cantidad de veces que esta tarea fue ejecutada.
		 * El resultado de la [[Tarea]] dada es el valor contenido en el [[Option]] resultante.
		 * ADVERTENCIA: la ejecución de la tarea dada sería eterna si el resultado de esta tarea nunca da un resultado tal que al aplicarle la función recibida de un [[Option]] definido. */
		def repetidaHastaCondicion[B](condicion: (A, Int) => Option[B]): Tarea[B] = {
			new Tarea[B] {
				var cantEjecucionesRealizadas: Int = 0;

				override def realizar(llameYo: Boolean)(cuandoCompleta: Try[B] => Unit): Unit = {
					def trabajo(nivelRecursion: Int): Unit = {
						tarea.realizar(llameYo = true) { tryA =>
							cantEjecucionesRealizadas += 1;
							
							val tryResultadoCondicion = for {
								a <- tryA
								resultadoCondicion <- Try(condicion(a, cantEjecucionesRealizadas))
							} yield resultadoCondicion
							
							tryResultadoCondicion match {
								case Success(resultadoCondicion) =>
									resultadoCondicion match {
										case Some(b) =>
											cuandoCompleta(Success(b));

										case None =>
											// Limitar el nivel de recursión para evitar desborde del stack.
											if (nivelRecursion < 5) { // TODO hacer que el nivel de recursión máximo sea configurable
												trabajo(nivelRecursion + 1)
											} else {
												actor.self ! Efectuar(() => trabajo(0))
											}
									}
								case Failure(e) =>
									cuandoCompleta(Failure(e))
							}
						}
					}

					if (llameYo) trabajo(0);
					else actor.self ! Efectuar(() => trabajo(0));
				}
			}
		}


		/** Da una nueva [[Tarea]] que repite esta [[Tarea]] hasta que la [[PartialFunction]] recibida este definida en el resultado que de esta [[Tarea]].
		 * El resultado de la [[Tarea]] dada es el resultado de aplicar la [[PartialFunction]] recibida sobre el resultado de esta [[Tarea]].
		 * ADVERTENCIA: la ejecución de la tarea dada sería eterna si el resultado de esta tarea nunca da un resultado tal que la [[PartialFunction]] recibida este definida. */
		def repetidaHastaDefinida[B](pf: PartialFunction[(A, Int), B]): Tarea[B] = {
			this.repetidaHastaCondicion(Function.untupled(pf.lift))
		}

		/** Da una nueva [[Tarea]] que hace lo siguiente: mientras la evaluación de la función `condiciónNegada` recibida de [[None]] efectúa esta tarea repetidamente.
		 * Dicho específicamente, cuando la tarea dada es ejecutada ocurre lo siguiente:
		 * - (1) Primero se evalúa la expresión `condiciónNegada(a0, 0)`. Si resulta un:
		 * -	[[Some]], la tarea dada completa exitosamente dando el contenido de dicho [[Some]].
		 * -	[[None]], efectúa esta tarea y luego evalúa la expresión `condiciónNegada(a1, 1)` donde a1 es el valor dado por esta tarea en su primera ejecución exitosa. Si resulta un:
		 * - 		[[Some]], la tarea dada completa exitosamente dando el contenido de dicho [[Some]].
		 * -		[[None]], efectúa esta tarea y luego evalúa la función `condiciónNegada(a2, 2)` donde a2 es el resultado de la segunda ejecución exitosa de esta tarea.
		 * Y así sucesivamente. */
		def repetidaMientrasCondicion[SA >: A, B](a0: SA)(condicionNegada: (SA, Int) => Option[B]): Tarea[B] = {
			try {
				condicionNegada(a0, 0) match {
					case Some(b) =>
						Tarea.trivial(b)
					case None =>
						tarea.repetidaHastaCondicion(condicionNegada)
				}
			} catch {
				case NonFatal(e) => Tarea.fallida(e)
			}
		}

		/** Da una nueva [[Tarea]] que hace lo siguiente: mientras la función parcial recibida no esté definida efectúa esta tarea repetidamente.
		 * Dicho específicamente, cuando la tarea dada es ejecutada ocurre lo siguiente:
		 * - (1) Primero se investiga si la función parcial esta definida en (a0, 0). Si:
		 * -	está definida, la tarea dada completa exitosamente dando el resultado de aplicarla.
		 * -	no está definida, efectúa esta tarea y luego investiga si la función parcial está definida en (a1, 1) donde a1 es el valor dado por esta tarea en su primera ejecución exitosa. Si:
		 * - 		está definida, la tarea dada completa exitosamente dando el resultado de aplicarla.
		 * -		no está definia, efectúa esta tarea y luego investiga si la función parcial esta definida en (a2, 2) donde a2 es el resultado de la segunda ejecución exitosa de esta tarea.
		 * Y así sucesivamente. */
		def repetidaMientrasIndefinida[SA >: A, B](a0: SA)(pf: PartialFunction[(SA, Int), B]): Tarea[B] = {
			this.repetidaMientrasCondicion(a0)(Function.untupled(pf.lift));
		}

		/** Da una nueva [[Tarea]] que repite a esta [[Tarea]] intercaladamente con la tarea dada por el `ctorTareaIntercalada`, hasta que el resultado de esta [[Tarea]] y la cantidad de veces que esta tarea fue efectuada sean tales que aplicarle la función `condición` de un [[Right]].
		 * La tarea intercalada es creada y ejecutada después de esta tarea, y solo cuando el resultado de esta tarea sea tal que aplicándole la función `condición` de un [[Left]].
		 * El resultado de la [[Tarea]] dada es el valor contenido en el [[Right]] dado por la `condición` que detuvo la repetición.
		 * ADVERTENCIA: la ejecución de la tarea dada sería eterna si el resultado de esta tarea siempre da un resultado tal que al aplicarle la función `condición` de un [[Left]]. */
		def repetidaIntercaladamenteHastaCondicion[B, C](condicion: (A, Int) => Either[C, B])(ctorTareaIntercalada: C => Tarea[Unit]): Tarea[B] = {
			this.repetidaHastaCondicion { (a, cantEjecucionesEfectuadas) =>
				condicion(a, cantEjecucionesEfectuadas) match {
					case Right(b) =>
						Some(b)

					case Left(c) =>
						ctorTareaIntercalada(c).efectuarYManejar(llameYo = true) {
							case NonFatal(e) => throw e
						}
						None
				}
			}
		}

		/** Da una nueva [[Tarea]] que hace lo mismo que esta [[Tarea]], pero si falla y el predicado `p` da `true` vuelve a intentar `cantReintentos` veces.
		 * El resultado de la nueva tarea dada es el que de esta tarea en el último intento, sea exitoso o fallido.
		 */
		def reintentadaSiFallaPor(cantReintentos: Int)(p: Throwable => Boolean): Tarea[A] =
			new Tarea[A] {
				override def realizar(llameYo: Boolean)(cuandoCompleta: Try[A] => Unit): Unit = {
					tarea.realizar(llameYo) {
						case s@Success(_) => cuandoCompleta(s)
						case f@Failure(e) =>
							if (cantReintentos <= 0)
								cuandoCompleta(f)
							else
								try {
									if (p(e))
										this.reintentadaSiFallaPor(cantReintentos - 1)(p).realizar(llameYo = true)(cuandoCompleta)
									else
										cuandoCompleta(f)
								} catch {
									case NonFatal(nf) => cuandoCompleta(Failure(nf))
								}
					}
				}
			}

		/** Casts the type-path of this [[Tarea]] to the received actor. Usar con cautela.
		 * Esta operación no hace nada en tiempo de ejecución. Solo engaña al compilador para evitar que chille cuando se opera con [[Tarea]]s que tienen distinto type-path pero se sabe que corresponden al mismo actor ejecutor.
		 * Se decidió hacer que [[Tarea]] sea un inner class del actor ejecutor para detectar en tiempo de compilación cuando se usa una instancia fuera de dicho actor.
		 * Usar el type-path checking para detectar en tiempo de compilación cuando una [[Tarea]] está siendo usada fuera del actor ejecutor es muy valioso, pero tiene un costo: El chequeo de type-path es más estricto de lo necesario para este propósito y, por ende, el compilador reportará errores de tipo en situaciones donde se sabe que el actor ejecutor es el correcto. Esta operación ([[castTypePath()]]) está para tratar esos casos.
		 */
		def castTypePath[E <: AmigoFutures](actorEjecutor: E): actorEjecutor.Tarea[A] = {
			assert(actorEjecutor eq actor);
			this.asInstanceOf[actorEjecutor.Tarea[A]]
		}
	}


	/** Tarea que al ejecutarla da como resultado el valor recibido.
	 *
	 * $workaroundAbstractMethodError
	 */
	class TareaTrivial[A](ta: Try[A]) extends Tarea[A] {
		override def realizar(llameYo: Boolean)(cuandoCompleta: Try[A] => Unit): Unit =
			if (llameYo)
				cuandoCompleta(ta)
			else
				actor.self ! Efectuar(() => cuandoCompleta(ta))

		override def correr(llameYo: Boolean): Future[A] =
			Future.fromTry(ta)
	}

	/** Tarea que al ejecutarla:
	 *  - primero programa la ejecución por parte de este actor de la `acción` recibida;
	 *  - ultimo llama a `cuandoCompleta` con:
	 *  	- el resultado de la `acción` si termina normalmente;
	 *  	- la excepción lanzada por la `acción` si termina abruptamente.
	 *
	 * $workaroundAbstractMethodError
	 *
	 * @param accion acción realizada por esta [[Tarea]]. $ejecutadaPorEsteActor $estaCuidada
	 */
	class TareaPropia[A](accion: () => Try[A]) extends Tarea[A] {
		override def realizar(llameYo: Boolean)(cuandoCompleta: Try[A] => Unit): Unit = {
			@inline
			def accionProtegida =
				try accion()
				catch {
					case NonFatal(e) => Failure(e)
				}

			if (llameYo) {
				cuandoCompleta(accionProtegida)
			} else {
				actor.self ! Efectuar(() => cuandoCompleta(accionProtegida))
			}
		}
	}

	/** Tarea cuya acción encapsulada NO es efectuada por este [[Actor]] y cuyo resultado esta representado por un [[Future]].
	 * Ejecutar esta [[Tarea]] provoca que el call-back `cuandoCompleta` sea llamado cuando el `future` complete, sea exitosa o fallidamente.
	 *
	 * $cuandoCompletaEjecutadaPorEsteActor
	 * $workaroundAbstractMethodError
	 *
	 * @param future que tendrá el resultado de la tarea.
	 */
	class TareaAjena[A](future: Future[A]) extends Tarea[A] {
		override def realizar(llameYo: Boolean)(cuandoCompleta: Try[A] => Unit): Unit =
			future.onComplete(cuandoCompleta)(ejecutadoPorEsteActor)
	}

	/** [[Tarea]] que al ejecutarla:
	 *  - primero ejecuta la `tarea` recibida;
	 *  - segundo aplica la función `f` al resultado y, si la aplicación termina normalmente:
	 *  	- tercero ejecuta la tarea creada;
	 *  - último llama a `cuandoCompleta` con:
	 *  	- la excepción lanzada en el segundo paso por la operación `ctorTarea` si termina abruptamente;
	 *  	- o el resultado de la ejecución del tercer paso.
	 *
	 * $cuandoCompletaEjecutadaPorEsteActor
	 * $workaroundAbstractMethodError
	 *
	 * @param f función que se aplica al resultado de esta [[Tarea]] para crear la [[Tarea]] que se ejecutará después. $ejecutadaPorEsteActor $estaCuidada
	 */
	class TareaTransformada[A, B](tarea: Tarea[A], f: Try[A] => Try[B]) extends Tarea[B] {
		override def realizar(llameYo: Boolean)(cuandoCompleta: Try[B] => Unit): Unit = {
			tarea.realizar(llameYo)(ta => cuandoCompleta {
				try f(ta)
				catch {
					case NonFatal(e) => Failure(e);
				}
			}
			);
		}
	}

	/** Tarea que representa la composición de dos tareas donde la segunda es creada a partir del resultado de la primera.
	 *
	 * $cuandoCompletaEjecutadaPorEsteActor
	 * $workaroundAbstractMethodError
	 *
	 * @param tarea1     tarea que se ejecutaría antes
	 * @param ctorTarea2 constructor de la tarea que se ejecutaría después. Este constructor es ejecutado por este actor cuando la primer tarea terminó.
	 */
	class TareaCompuesta[A, B](tarea1: Tarea[A], ctorTarea2: Try[A] => Tarea[B]) extends Tarea[B] {
		override def realizar(llameYo: Boolean)(cuandoCompleta: Try[B] => Unit): Unit =
			tarea1.realizar(llameYo) { ta =>
				ctorTarea2(ta).realizar(llameYo = true)(cuandoCompleta);
			}
	}

	/** [[Tarea]] que al ejecutarla:
	 *  - primero ejecuta las `tareaA` y `tareaB` en simultaneo (en oposición a esperar que termine una para ejecutar la otra)
	 *  - segundo, cuando ambas tareas terminan, sea exitosamente o fallidamente, se aplica la función `f` al resultado de las dos tareas.
	 *  - ultimo llama a `cuandooCompleta`con:
	 *  	- la excepción lanzada por la función `f` si termina abruptamente;
	 *  	- o el resultado de f si termina normalmente
	 *      $cuandoCompletaEjecutadaPorEsteActor
	 *      $workaroundAbstractMethodError
	 */
	class TareaBifurcada[A, B, C](tareaA: Tarea[A], tareaB: Tarea[B], f: (Try[A], Try[B]) => Try[C]) extends Tarea[C] {
		override def realizar(llameYo: Boolean)(cuandoCompleta: Try[C] => Unit): Unit = {
			var ota: Option[Try[A]] = None;
			var otb: Option[Try[B]] = None;
			tareaA.realizar(llameYo) { ta =>
				otb match {
					case None => ota = Some(ta)
					case Some(tb) => cuandoCompleta {
						try {
							f(ta, tb)
						} catch {
							case NonFatal(e) => Failure(e)
						}
					}
				}
			}
			tareaB.realizar(llameYo) { tb =>
				ota match {
					case None => otb = Some(tb)
					case Some(ta) => cuandoCompleta {
						try {
							f(ta, tb)
						} catch {
							case NonFatal(e) => Failure(e)
						}
					}
				}
			}
		}
	}

	/** Tarea que al ejecutarla construye y ejecuta tareas repetidas veces mientras se cumpla la `condicion`.
	 * Precisamente, cuando se ejecuta hace lo siguiente:
	 *  - evalúa `condicion(a0)` y si da:
	 *  	- `Left(tryB)` la tarea bucle completa con `tryB`
	 *  	- `Right(tareaA)` ejecuta la `tareaA` y si:
	 *  		- completa con falla, la tarea bucle completa con esa falla
	 *  		- completa exitosamente con el valor `a1`, se vuelve al primer paso pero reemplazando `a0` con `a1`.
	 *
	 * $cuandoCompletaEjecutadaPorEsteActor
	 * $workaroundAbstractMethodError
	 */
	case class TareaBucle[A, B](a0: A)(condicion: A => Either[Try[B], Tarea[A]]) extends Tarea[B] {
		override def realizar(llameYo: Boolean)(cuandoCompleta: Try[B] => Unit): Unit = {
			def trabajo(): Unit = try {
				condicion(a0) match {
					case Left(tryB) =>
						cuandoCompleta(tryB);
					case Right(tareaA) =>
						tareaA.realizar(llameYo = true) {
							case Failure(e) =>
								cuandoCompleta(Failure(e));
							case Success(a1) =>
								TareaBucle(a1)(condicion).realizar(llameYo = true)(cuandoCompleta);
						}
				}
			} catch {
				case NonFatal(e) => cuandoCompleta(Failure(e));
			}

			if (llameYo) trabajo();
			else actor.self ! Efectuar(() => trabajo());
		}
	}

	/** Análogo al [[Promise]] pero para [[Tarea]]s en lugar de [[Future]]s. */
	class Promesa[A] { promesa =>
		private var oResultado: Option[Try[A]] = None;
		private var observadoresDeCuandoCompleta: List[Try[A] => Unit] = Nil;

		def estaConcretada: Boolean = this.oResultado.isDefined;

		def estaPendiente: Boolean = this.oResultado.isEmpty;

		/** Da la tarea que esta promesa promete realizar. Esta tarea completaría cuando esta [[Promesa]] sea concretada. */
		val tarea: Tarea[A] = new Tarea[A] {
			def realizar(llameYo: Boolean)(cuandoCompleta: Try[A] => Unit): Unit = {
				def trabajo(): Unit = promesa.oResultado match {
					case Some(resultado) =>
						cuandoCompleta(resultado);
					case None =>
						promesa.observadoresDeCuandoCompleta = cuandoCompleta :: promesa.observadoresDeCuandoCompleta;
				}

				if (llameYo) trabajo();
				else actor.self ! Efectuar(() => trabajo());
			}
		}

		/** Provoca que la `tarea` que esta [[Promesa]] promete realizar complete. */
		def concretar(resultado: Try[A]): this.type = {
			actor.self ! Efectuar(() =>
				oResultado match {
					case None =>
						this.oResultado = Some(resultado);
						this.observadoresDeCuandoCompleta.foreach(_(resultado));
						// la lista de observadores quedó obsoleta. Borrarla para minimizar posibilidad de memory leak.
						this.observadoresDeCuandoCompleta = Nil
					case Some(_) =>
						throw new IllegalStateException("Esta promesa ya estaba cumplida");
				}
			);
			this;
		}

		/** Provoca que la `tarea` que esta [[Promesa]] promete realizar complete exitosamente. */
		def cumplir(resultado: A): this.type = this.concretar(Success(resultado));

		/** Provoca que la `tarea` que esta [[Promesa]] promete realizar complete fallidamente. */
		def fallar(excepcion: Throwable): this.type = this.concretar(Failure(excepcion));

		/** Ejecuta la `otraTarea` recibida y, cuando completa, concreta esta [[Promesa]] con el resultado. */
		def concretarCon(otraTarea: Tarea[A]): this.type = {
			if (otraTarea ne this.tarea)
				otraTarea.realizar(llameYo = false)(concretar);
			this
		}
	}
}

object AmigoFuturesYTimers {
	private case class IdTemporizador(num: Long);
}

trait AmigoFuturesYTimers extends AmigoFutures with Timers { actor: Actor =>

	import AmigoFuturesYTimers.*;

	private var secuenciador: Long = 0;

	private def genIdTemporizador(): IdTemporizador = {
		secuenciador += 1;
		IdTemporizador(secuenciador)
	}

	/** Truco para agregar operaciones al objeto [[AmigoFutures.Tarea]]. Para que funcione se requiere que esta clase esté importada. */
	implicit class PimpTareaCompanion(companion: Tarea.type) {
		/** Crea una tarea, llamémosla "bucle", que al ejecutarla ejecuta la `tarea` supervisada recibida y, si consume mas tiempo que el margen recibido, la vuelve a ejecutar. Este ciclo se repite hasta que el tiempo que consume la ejecución de la tarea supervisada no supere el margen, o se acaben los reintentos.
		 * La ejecución de la tarea bucle completará cuando:
		 * - el tiempo que demora la ejecución de la tarea supervisada en completar esta dentro del margen, en cuyo caso el resultado de la tarea bucle sería `Some(resultadoTareaMonitoreada)`,
		 * - se acaben los reintentos, en cuyo caso el resultado de la tarea bucle sería `None`.
		 */
		def reintentarSiTranscurreMargen[A](cantReintentos: Int, margen: FiniteDuration)(tarea: Tarea[Try[A]]): Tarea[Option[A]] = {
			companion.reintenta[Unit, A](cantReintentos)(
				tarea.conMargen(margen).transform {

					case Success(ota) => ota match {

						case Some(Success(a)) => Success(Right(a))

						case Some(Failure(falla)) => Failure(falla)

						case None => Success(Left(()))
					}

					case Failure(falla) => Failure(falla);
				}
			).map {
				case Right(a) => Some(a);
				case Left(_) => None
			}
		}

		/** Crea una tarea que ejecuta la tarea recibida mientras el resultado de ella sea `None` y no se supere la `maximaCantEjecuciones` indicada; esperando la `pausa` indicada entre el fin de una ejecución y el comienzo de la siguiente. */
		def repitePausadamenteMientrasResultadoVacio[A](maximaCantEjecuciones: Int, pausa: FiniteDuration)(tarea: Tarea[Option[Try[A]]]): Tarea[Option[A]] =
			new TareaBuclePausado[A](maximaCantEjecuciones, pausa)(tarea)

		/** Crea una tarea que duerme durante la `duración` recibida. */
		def duerme(duracion: FiniteDuration): Tarea[Unit] = {
			new Promesa[Unit].tarea.conMargen(duracion).map(_ => ())
		}
	}

	/** Truco para agregar operaciones a las instancias de [[Tarea]]. Para que funcione se requiere que esta clase esté importada. */
	implicit class PimpTarea[A](tarea: Tarea[A]) {
		/** Da una [[Tarea]] que al ejecutarla:
		 *  - inicia simultáneamente la ejecución de esta [[Tarea]] y un temporizador con el `margen` de tiempo recibido. Si:
		 *  	- el margen de tiempo transcurre antes, llama a `cuandoCompleta` con `Success(None)`.
		 *  	- la ejecución completa normalmente con el resultado `a`, llama a `cuandoCompleta` con `Success(Some(a))`
		 *  	- la ejecución termina abruptamente, llama a `cuandoCompleta` con `Failure(causa)`.
		 */
		def conMargen(margen: FiniteDuration): Tarea[Option[A]] =
			new TareaConMargen[A](tarea, margen);
	}

	/** [[Tarea]] que al ejecutarla:
	 *  - inicia simultáneamente la ejecución de la [[Tarea]] `tarea` y un temporizador con el `margen` de tiempo recibido. Si:
	 *  	- la ejecución de la tarea completa antes, llama a `cuandoCompleta` con el resultado, sea exitoso o fallido, envuelto con [[Some]].
	 *  	- el margen de tiempo transcurre antes, llama a `cuandoCompleta` con `Success(None)`.
	 *
	 * $workaroundAbstractMethodError
	 */
	class TareaConMargen[A](tarea: Tarea[A], margen: FiniteDuration) extends Tarea[Option[A]] {
		def realizar(llameYo: Boolean)(cuandoCompleta: Try[Option[A]] => Unit): Unit = {
			def trabajo(): Unit = {
				var haTranscurrido = false;
				var haCompletado = false;
				val idTemporizador = genIdTemporizador();
				tarea.efectuar(llameYo = true) { tryA =>
					if (!haTranscurrido) {
						timers.cancel(idTemporizador);
						haCompletado = true;
						cuandoCompleta(tryA map Some.apply)
					}
				}
				timers.startSingleTimer(
					idTemporizador,
					Efectuar { () =>
						if (!haCompletado) {
							haTranscurrido = true;
							cuandoCompleta(Success(None))
						}
					},
					margen
				);
			}

			if (llameYo) trabajo();
			else actor.self ! Efectuar(() => trabajo());
		}
	}

	class TareaBuclePausado[A](maximaCantEjecuciones: Int, pausa: FiniteDuration)(tarea: Tarea[Option[Try[A]]]) extends Tarea[Option[A]] {
		def realizar(llameYo: Boolean)(cuandoCompleta: Try[Option[A]] => Unit): Unit = {

			def bucle(maximaCantEjecuciones: Int): Unit = {
				tarea.efectuar(llameYo = true) {
					case Success(Some(Success(a))) => cuandoCompleta(Success(Some(a)))
					case Success(Some(Failure(e))) => cuandoCompleta(Failure(e))
					case Success(None) =>
						if (maximaCantEjecuciones > 1) {
							val idTemporizador = genIdTemporizador();
							timers.startSingleTimer(
								idTemporizador,
								Efectuar { () => bucle(maximaCantEjecuciones - 1) },
								pausa
							)
						} else
							cuandoCompleta(Success(None))
					case Failure(e) => cuandoCompleta(Failure(e))
				}
			}

			def trabajo(): Unit = {
				if (maximaCantEjecuciones <= 0) {
					cuandoCompleta(Success(None))
				} else {
					bucle(maximaCantEjecuciones);
				}
			}

			if (llameYo) trabajo();
			else actor.self ! Efectuar(() => trabajo());
		}
	}
}
