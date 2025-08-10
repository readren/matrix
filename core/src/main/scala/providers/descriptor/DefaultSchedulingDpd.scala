package readren.matrix
package providers.descriptor

import core.Matrix
import core.Matrix.{DoerProvider, DoerProviderDescriptor, DoerProvidersManager}
import providers.doer.SchedulingDoerProvider
import common.CompileTime


object DefaultSchedulingDpd extends DoerProviderDescriptor[SchedulingDoerProvider.ProvidedDoer](CompileTime.getTypeName[DefaultSchedulingDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[SchedulingDoerProvider.ProvidedDoer] = new SchedulingDoerProvider(false)
}
