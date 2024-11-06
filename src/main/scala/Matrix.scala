package readren.matrix

import readren.taskflow.{Doer, Maybe}

import java.net.{InetAddress, URI}


object Matrix {
	trait Aide {
		def reportFailure(cause: Throwable): Unit

		def buildDoerAssistantForAdmin(): Doer.Assistant
	}
}

class Matrix(val name: String, aide: Matrix.Aide) { thisMatrix =>

	import Matrix.*

	//	private val msgHandlerExecutorService = new MsgHandlerExecutorService

	/**
	 * The array with all the [[MatrixAdmin]] instances of this [[Matrix]]. */
	val matrixAdmins: IArray[MatrixAdmin] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		val doerAssistant = aide.buildDoerAssistantForAdmin()
		IArray.fill(availableProcessors)(new MatrixAdmin(doerAssistant, thisMatrix))
	}

	val progenitor: Spawner[MatrixAdmin] = new Spawner(Maybe.empty, matrixAdmins(0), 0)

	inline def pickAdmin(serialNumber: Reactant.SerialNumber): MatrixAdmin = matrixAdmins(serialNumber % matrixAdmins.length)


	val uri: URI = {
		val host = InetAddress.getLocalHost.getHostAddress // Or use getHostAddress for IP

		// Define the URI components
		val scheme = "http" // You can change this to "https" if needed
		val port = 8080 // Replace with the desired port, or omit it if not needed
		val path = s"/$name/" // Replace with your specific path

		// Construct the URI
		new URI(scheme, null, host, port, path, null, null)
	}
}
