package readren.matrix
package providers.descriptor

import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}

import readren.common.CompileTime.getTypeName
import readren.sequencer.providers.RoundRobinDp
import readren.sequencer.{Doer, DoerProvider}


object DefaultRoundRobinDpd extends DoerProviderDescriptor[Doer](getTypeName[DefaultRoundRobinDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[Doer] = new RoundRobinDp.Impl()
}
