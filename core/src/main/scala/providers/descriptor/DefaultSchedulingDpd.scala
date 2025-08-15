package readren.matrix
package providers.descriptor

import core.Matrix
import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import readren.sequencer.providers.{DoerProvider, SchedulingDp}

import readren.common.CompileTime.getTypeName


object DefaultSchedulingDpd extends DoerProviderDescriptor[SchedulingDp.SchedulingDoerFacade](getTypeName[DefaultSchedulingDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[SchedulingDp.SchedulingDoerFacade] = new SchedulingDp(false)
}
