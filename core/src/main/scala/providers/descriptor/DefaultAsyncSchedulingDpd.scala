package readren.matrix
package providers.descriptor

import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}

import readren.common.CompileTime.getTypeName
import readren.sequencer.DoerProvider
import readren.sequencer.providers.CooperativeWorkersWithAsyncSchedulerDp


object DefaultAsyncSchedulingDpd extends DoerProviderDescriptor[CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultAsyncSchedulingDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade] = new CooperativeWorkersWithAsyncSchedulerDp.Impl(false)
}
