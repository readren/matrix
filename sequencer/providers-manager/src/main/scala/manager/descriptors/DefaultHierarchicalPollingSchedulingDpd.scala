package readren.sequencer
package manager.descriptors

import manager.{DoerProviderDescriptor, DoerProvidersManager}
import providers.CooperativeWorkersWithHierarchicalPollingSchedulerDp

import readren.common.CompileTime.getTypeName
import readren.sequencer.providers

object DefaultHierarchicalPollingSchedulingDpd extends DoerProviderDescriptor[CooperativeWorkersWithHierarchicalPollingSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultHierarchicalPollingSchedulingDpd.type]) {
	override type Tag = String
	override type DP = providers.CooperativeWorkersWithHierarchicalPollingSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new providers.CooperativeWorkersWithHierarchicalPollingSchedulerDp.Impl(false)
}
