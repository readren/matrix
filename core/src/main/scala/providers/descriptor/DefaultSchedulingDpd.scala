package readren.matrix
package providers.descriptor

import common.CompileTime
import core.Matrix
import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import providers.assistant.{DoerProvider, SchedulingDap}


object DefaultSchedulingDpd extends DoerProviderDescriptor[SchedulingDap.SchedulingDoerFacade](CompileTime.getTypeName[DefaultSchedulingDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[SchedulingDap.SchedulingDoerFacade] = new SchedulingDap(false)
}
