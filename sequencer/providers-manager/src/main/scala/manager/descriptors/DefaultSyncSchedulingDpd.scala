package readren.sequencer
package manager
package descriptors

import providers.CooperativeWorkersWithSyncSchedulerDp

import readren.common.CompileTime.getTypeName

object DefaultSyncSchedulingDpd extends DoerProviderDescriptor[CooperativeWorkersWithSyncSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultSyncSchedulingDpd.type]) {
	override type Tag = String
	override type DP = CooperativeWorkersWithSyncSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new CooperativeWorkersWithSyncSchedulerDp.Impl(false)
}
