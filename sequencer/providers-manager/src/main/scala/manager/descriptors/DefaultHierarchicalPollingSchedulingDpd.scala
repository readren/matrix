package readren.sequencer
package manager.descriptors

import manager.{DoerProviderDescriptor, DoerProvidersManager}
import providers.CooperativeHierarchicalPollingSchedulerDp

import readren.common.CompileTime.getTypeName
import readren.sequencer.providers

object DefaultHierarchicalPollingSchedulingDpd extends DoerProviderDescriptor[CooperativeHierarchicalPollingSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultHierarchicalPollingSchedulingDpd.type]) {
	override type Tag = String
	override type DP = providers.CooperativeHierarchicalPollingSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new providers.CooperativeHierarchicalPollingSchedulerDp.Impl(false)
}
