package readren.sequencer
package manager
package descriptors

import providers.CooperativeWorkersDp

import readren.common.CompileTime.getTypeName

object DefaultCooperativeWorkersDpd extends DoerProviderDescriptor[CooperativeWorkersDp.DoerFacade](getTypeName[DefaultCooperativeWorkersDpd.type]) {
	override type Tag = String
	override type DP = CooperativeWorkersDp.Impl

	override def build(owner: DoerProvidersManager): DP = new CooperativeWorkersDp.Impl(false)
}
