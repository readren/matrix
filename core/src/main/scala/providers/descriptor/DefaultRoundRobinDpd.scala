package readren.matrix
package providers.descriptor

import common.CompileTime
import core.Matrix
import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import providers.assistant.{DoerProvider, RoundRobinDap}


object DefaultRoundRobinDpd extends DoerProviderDescriptor[RoundRobinDap.ProvidedDoer](CompileTime.getTypeName[DefaultRoundRobinDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[RoundRobinDap.ProvidedDoer] = new RoundRobinDap()
}
