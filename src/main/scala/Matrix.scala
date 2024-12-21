package readren.matrix

import readren.taskflow.{Doer, Maybe}

import java.util.concurrent.atomic.AtomicLong


object Matrix {
	trait DoerAssistantProvider {
		def provide(serial: MatrixDoer.Id): Doer.Assistant
	}

	trait Aide[DP <: DoerAssistantProvider] {
		def buildLogger(owner: Matrix[DP]): Logger
		def buildDoerAssistantProvider(owner: Matrix[DP]): DP
	}
}

class Matrix[+DP <: Matrix.DoerAssistantProvider](name: String, aide: Matrix.Aide[DP]) extends AbstractMatrix(name) { thisMatrix =>

	import Matrix.*
	
	private val matrixDoerIdSequencer = new AtomicLong(0)

	val doerAssistantProvider: DP = aide.buildDoerAssistantProvider(thisMatrix)

	override def provideDoer(): MatrixDoer = {
		val serial = matrixDoerIdSequencer.getAndIncrement()
		new MatrixDoer(serial, doerAssistantProvider.provide(serial), thisMatrix) 
	}

	override val doer: MatrixDoer = provideDoer()

	override protected val spawner: Spawner[doer.type] = new Spawner(Maybe.empty, doer, 0)
	
	override val logger: Logger = aide.buildLogger(this)

}
