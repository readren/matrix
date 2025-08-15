package readren.matrix
package providers.descriptor

import core.Matrix
import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import providers.{DoerProvider, RoundRobinDp}

import readren.common.CompileTime.getTypeName


object DefaultRoundRobinDpd extends DoerProviderDescriptor[RoundRobinDp.ProvidedDoer](getTypeName[DefaultRoundRobinDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[RoundRobinDp.ProvidedDoer] = new RoundRobinDp()
}
