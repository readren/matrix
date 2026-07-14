package readren.sequencer
package manager.descriptors

import manager.{DoerProviderDescriptor, DoerProvidersManager}
import providers.{CooperativeHierarchicalPollingSchedulerDp, CooperativeLocalPollingSchedulerDp}

import readren.common.CompileTime.getTypeName
import readren.sequencer.providers

object DefaultLocalPollingSchedulingDpd extends DoerProviderDescriptor[CooperativeLocalPollingSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultLocalPollingSchedulingDpd.type]) {
	override type Tag = String
	override type DP = providers.CooperativeLocalPollingSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new providers.CooperativeLocalPollingSchedulerDp.Impl(false)
}
