package readren.matrix
package providers.descriptor

import core.Matrix
import core.Matrix.{DoerProvider, DoerProviderDescriptor, DoerProvidersManager}
import providers.doer.LeastLoadedFixedWorkerDoerProvider
import utils.CompileTime


object DefaultLeastLoadedFixedWorkerDpd extends DoerProviderDescriptor[LeastLoadedFixedWorkerDoerProvider.ProvidedDoer](CompileTime.getTypeName[DefaultLeastLoadedFixedWorkerDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[LeastLoadedFixedWorkerDoerProvider.ProvidedDoer] = new LeastLoadedFixedWorkerDoerProvider()
}
