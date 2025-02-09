package readren.matrix
package providers.descriptor

import core.Matrix
import core.Matrix.{DoerProvider, DoerProviderDescriptor, DoerProvidersManager}
import providers.doer.CooperativeWorkersDoerProvider
import utils.CompileTime


object DefaultCooperativeWorkersDpd extends DoerProviderDescriptor[CooperativeWorkersDoerProvider.ProvidedDoer](CompileTime.getTypeName[DefaultCooperativeWorkersDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[CooperativeWorkersDoerProvider.ProvidedDoer] = new CooperativeWorkersDoerProvider(false)
}
