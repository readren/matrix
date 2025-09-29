package readren.sequencer
package manager
package descriptors

import readren.common.CompileTime.getTypeName
import readren.sequencer.providers.LeastLoadedFixedWorkerDp

object DefaultLeastLoadedFixedWorkerDpd extends DoerProviderDescriptor[Doer](getTypeName[DefaultLeastLoadedFixedWorkerDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[Doer] = new LeastLoadedFixedWorkerDp.Impl()
}
