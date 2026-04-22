package readren.sequencer
package manager
package descriptors

import providers.CooperativeWorkersWithThreadDrivenSchedulerDp

import readren.common.CompileTime.getTypeName

object DefaultThreadDrivenSchedulingDpd extends DoerProviderDescriptor[CooperativeWorkersWithThreadDrivenSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultThreadDrivenSchedulingDpd.type]) {
	override type Tag = String
	override type DP = CooperativeWorkersWithThreadDrivenSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new CooperativeWorkersWithThreadDrivenSchedulerDp.Impl(false)
}
