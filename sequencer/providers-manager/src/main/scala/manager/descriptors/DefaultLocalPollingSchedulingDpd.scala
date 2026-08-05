package readren.sequencer
package manager.descriptors

import manager.{DoerProviderDescriptor, DoerProvidersManager}
import providers.CooperativeLocalPollingSchedulerDp

import readren.common.CompileTime.getTypeName

object DefaultLocalPollingSchedulingDpd extends DoerProviderDescriptor[CooperativeLocalPollingSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultLocalPollingSchedulingDpd.type]) {
	override type Tag = String
	override type DP = providers.CooperativeLocalPollingSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new providers.CooperativeLocalPollingSchedulerDp.Impl(false)
}
