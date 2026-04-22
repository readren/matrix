package readren.sequencer
package manager
package descriptors

import providers.CooperativeWorkersWithPollingSchedulerDp

import readren.common.CompileTime.getTypeName

object DefaultPollingSchedulingDpd extends DoerProviderDescriptor[CooperativeWorkersWithPollingSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultPollingSchedulingDpd.type]) {
	override type Tag = String
	override type DP = CooperativeWorkersWithPollingSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new CooperativeWorkersWithPollingSchedulerDp.Impl(false)
}
