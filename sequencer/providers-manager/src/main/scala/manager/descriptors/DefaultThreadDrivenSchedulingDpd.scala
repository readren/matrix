package readren.sequencer
package manager
package descriptors

import providers.CooperativeThreadDrivenSchedulerDp

import readren.common.CompileTime.getTypeName

object DefaultThreadDrivenSchedulingDpd extends DoerProviderDescriptor[CooperativeThreadDrivenSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultThreadDrivenSchedulingDpd.type]) {
	override type Tag = String
	override type DP = CooperativeThreadDrivenSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new CooperativeThreadDrivenSchedulerDp.Impl(false)
}
