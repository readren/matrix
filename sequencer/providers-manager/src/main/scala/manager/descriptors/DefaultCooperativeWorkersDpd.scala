package readren.sequencer
package manager
package descriptors

import readren.common.CompileTime.getTypeName
import readren.sequencer.providers.CooperativeWorkersDp

object DefaultCooperativeWorkersDpd extends DoerProviderDescriptor[CooperativeWorkersDp.DoerFacade](getTypeName[DefaultCooperativeWorkersDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[CooperativeWorkersDp.DoerFacade] = new CooperativeWorkersDp.Impl(false)
}
