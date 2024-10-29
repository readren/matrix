package readren.matrix

import readren.taskflow.Doer

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

object Matrix {
	trait Aide extends Progenitor.Aide {
		def reportFailure(cause: Throwable): Unit
	}


}

class Matrix(name: String, aide: Matrix.Aide) extends Progenitor(0, aide) { thisMatrix =>

	import Matrix.*


}
