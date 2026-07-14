package readren.sequencer
package manager
package descriptors

import providers.CooperativeFlatPollingSchedulerDp

import readren.common.CompileTime.getTypeName

object DefaultFlatPollingSchedulingDpd extends DoerProviderDescriptor[CooperativeFlatPollingSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultFlatPollingSchedulingDpd.type]) {
	override type Tag = String
	override type DP = CooperativeFlatPollingSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new CooperativeFlatPollingSchedulerDp.Impl(false)
}
