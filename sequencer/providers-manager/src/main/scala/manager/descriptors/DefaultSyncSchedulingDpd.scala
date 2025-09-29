package readren.sequencer
package manager
package descriptors

import readren.common.CompileTime.getTypeName
import readren.sequencer.providers.CooperativeWorkersWithSyncSchedulerDp

object DefaultSyncSchedulingDpd extends DoerProviderDescriptor[CooperativeWorkersWithSyncSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultSyncSchedulingDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[CooperativeWorkersWithSyncSchedulerDp.SchedulingDoerFacade] = new CooperativeWorkersWithSyncSchedulerDp.Impl(false)
}
