package readren.matrix
package providers.descriptor

import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}

import readren.common.CompileTime.getTypeName
import readren.sequencer.DoerProvider
import readren.sequencer.providers.{CooperativeWorkersWithAsyncSchedulerDp, CooperativeWorkersWithSyncSchedulerDp}


object DefaultSyncSchedulingDpd extends DoerProviderDescriptor[CooperativeWorkersWithSyncSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultSyncSchedulingDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[CooperativeWorkersWithSyncSchedulerDp.SchedulingDoerFacade] = new CooperativeWorkersWithSyncSchedulerDp.Impl(false)
}
