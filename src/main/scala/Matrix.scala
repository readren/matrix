package readren.matrix

import readren.taskflow.Maybe


object Matrix {
	trait DoerProvider {
		type Doer <: MatrixDoer
		def provide(): MatrixDoer
	}

	trait Aide[DP <: DoerProvider] {
		def buildLogger(owner: Matrix[DP]): Logger
		def buildDoerProvider(owner: Matrix[DP]): DP
	}
}

class Matrix[+DP <: Matrix.DoerProvider](name: String, aide: Matrix.Aide[DP]) extends AbstractMatrix(name) { thisMatrix =>

	import Matrix.*

	val doerProvider: DP = aide.buildDoerProvider(thisMatrix)

	override def provideDoer(): MatrixDoer = doerProvider.provide()

	override val doer: MatrixDoer = provideDoer()

	override protected val spawner: Spawner[doer.type] = new Spawner(Maybe.empty, doer, 0)
	
	override val logger: Logger = aide.buildLogger(this)

}
