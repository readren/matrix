package readren.matrix
package providers.descriptor

import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}

import readren.sequencer.providers.{DoerProvider, LeastLoadedFixedWorkerDp}
import readren.common.CompileTime.getTypeName
import readren.sequencer.Doer


object DefaultLeastLoadedFixedWorkerDpd extends DoerProviderDescriptor[Doer](getTypeName[DefaultLeastLoadedFixedWorkerDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[Doer] = new LeastLoadedFixedWorkerDp.Impl()
}
