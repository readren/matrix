package readren.sequencer
package manager.descriptors

import manager.{DoerProviderDescriptor, DoerProvidersManager}
import providers.{CooperativeHierarchicalPollingSchedulerDp, CooperativeShardedPollingSchedulerDp}

import readren.common.CompileTime.getTypeName
import readren.sequencer.providers

object DefaultShardedPollingSchedulingDpd extends DoerProviderDescriptor[CooperativeShardedPollingSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultShardedPollingSchedulingDpd.type]) {
	override type Tag = String
	override type DP = providers.CooperativeShardedPollingSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new providers.CooperativeShardedPollingSchedulerDp.Impl(false)
}
