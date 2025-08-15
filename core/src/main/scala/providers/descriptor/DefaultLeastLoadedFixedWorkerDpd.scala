package readren.matrix
package providers.descriptor

import common.CompileTime
import core.Matrix
import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import providers.assistant.{DoerProvider, LeastLoadedFixedWorkerDap}


object DefaultLeastLoadedFixedWorkerDpd extends DoerProviderDescriptor[LeastLoadedFixedWorkerDap.ProvidedDoer](CompileTime.getTypeName[DefaultLeastLoadedFixedWorkerDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[LeastLoadedFixedWorkerDap.ProvidedDoer] = new LeastLoadedFixedWorkerDap()
}
