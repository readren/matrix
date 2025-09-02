package readren.matrix
package providers.descriptor

import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}

import readren.sequencer.providers.LeastLoadedFixedWorkerDp
import readren.common.CompileTime.getTypeName
import readren.sequencer.{Doer, DoerProvider}


object DefaultLeastLoadedFixedWorkerDpd extends DoerProviderDescriptor[Doer](getTypeName[DefaultLeastLoadedFixedWorkerDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[Doer] = new LeastLoadedFixedWorkerDp.Impl()
}
