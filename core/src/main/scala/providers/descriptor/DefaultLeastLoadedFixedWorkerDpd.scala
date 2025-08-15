package readren.matrix
package providers.descriptor

import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import readren.sequencer.providers.{DoerProvider, LeastLoadedFixedWorkerDp}

import readren.common.CompileTime.getTypeName


object DefaultLeastLoadedFixedWorkerDpd extends DoerProviderDescriptor[LeastLoadedFixedWorkerDp.ProvidedDoer](getTypeName[DefaultLeastLoadedFixedWorkerDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[LeastLoadedFixedWorkerDp.ProvidedDoer] = new LeastLoadedFixedWorkerDp()
}
