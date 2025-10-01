package readren.sequencer
package manager
package descriptors

import providers.CooperativeWorkersWithAsyncSchedulerDp

import readren.common.CompileTime.getTypeName

object DefaultAsyncSchedulingDpd extends DoerProviderDescriptor[CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultAsyncSchedulingDpd.type]) {
	override type Tag = String
	override type DP = CooperativeWorkersWithAsyncSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new CooperativeWorkersWithAsyncSchedulerDp.Impl(false)
}
