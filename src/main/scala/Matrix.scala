package readren.matrix

import readren.taskflow.{Doer, Maybe}

import java.net.{InetAddress, URI}


object Matrix {
	trait Aide {
		def reportFailure(cause: Throwable): Unit

		def buildDoerAssistantForAdmin(adminId: Int): Doer.Assistant
	}
}

class Matrix(val name: String, aide: Matrix.Aide) { thisMatrix =>

	import Matrix.*

	/**
	 * The array with all the [[MatrixAdmin]] instances of this [[Matrix]]. */
	val matrixAdmins: IArray[MatrixAdmin] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		IArray.tabulate(availableProcessors) { index =>
			val adminId = index + 1
			new MatrixAdmin(adminId, aide.buildDoerAssistantForAdmin(adminId), thisMatrix)
		}
	}

	val admin: MatrixAdmin = matrixAdmins(0)

	private val spawner: Spawner[admin.type] = new Spawner(Maybe.empty, admin, 0)

	val uri: URI = {
		val host = InetAddress.getLocalHost.getHostAddress // Or use getHostAddress for IP

		// Define the URI components
		val scheme = "http" // You can change this to "https" if needed
		val port = 8080 // Replace with the desired port, or omit it if not needed
		val path = s"/$name/" // Replace with your specific path

		// Construct the URI
		new URI(scheme, null, host, port, path, null, null)
	}

	inline def pickAdmin(serialNumber: Reactant.SerialNumber): MatrixAdmin =
		matrixAdmins(serialNumber % matrixAdmins.length)

	/** thread-safe */
	def spawn[U, E <: U](reactantFactory: ReactantFactory)(initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]): admin.Duty[Endpoint[E]] =
		admin.Duty.mineFlat { () =>
			spawner.createReactant[U](reactantFactory, initialBehaviorBuilder)
				.map(_.endpointProvider.local[E])
		}
}
