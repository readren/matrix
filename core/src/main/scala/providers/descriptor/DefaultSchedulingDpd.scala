package readren.matrix
package providers.descriptor

import core.Matrix
import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import readren.sequencer.providers.CooperativeWorkersWithAsyncSchedulerDp

import readren.common.CompileTime.getTypeName
import readren.sequencer.DoerProvider


object DefaultSchedulingDpd extends DoerProviderDescriptor[CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade](getTypeName[DefaultSchedulingDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade] = new CooperativeWorkersWithAsyncSchedulerDp.Impl(false)
}
