package readren.matrix
package providers.descriptor

import core.Matrix
import core.Matrix.{DoerProvider, DoerProviderDescriptor, DoerProvidersManager}
import providers.doer.RoundRobinDoerProvider
import utils.CompileTime


object DefaultRoundRobinDpd extends DoerProviderDescriptor[RoundRobinDoerProvider.ProvidedDoer](CompileTime.getTypeName[DefaultRoundRobinDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[RoundRobinDoerProvider.ProvidedDoer] = new RoundRobinDoerProvider()
}
