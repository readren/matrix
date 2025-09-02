package readren.matrix
package providers.descriptor

import core.Matrix
import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import readren.sequencer.providers.CooperativeWorkersSchedulingDp

import readren.common.CompileTime.getTypeName
import readren.sequencer.DoerProvider


object DefaultSchedulingDpd extends DoerProviderDescriptor[CooperativeWorkersSchedulingDp.SchedulingDoerFacade](getTypeName[DefaultSchedulingDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[CooperativeWorkersSchedulingDp.SchedulingDoerFacade] = new CooperativeWorkersSchedulingDp.Impl(false)
}
