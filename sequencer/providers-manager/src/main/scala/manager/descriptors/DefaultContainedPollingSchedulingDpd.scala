package readren.sequencer
package manager.descriptors

import manager.{DoerProviderDescriptor, DoerProvidersManager}
import providers.CooperativeContainedPollingSchedulerDp

import readren.common.CompileTime.getTypeName

object DefaultContainedPollingSchedulingDpd extends DoerProviderDescriptor[CooperativeContainedPollingSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultContainedPollingSchedulingDpd.type]) {
	override type Tag = String
	override type DP = providers.CooperativeContainedPollingSchedulerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new providers.CooperativeContainedPollingSchedulerDp.Impl(false)
}
