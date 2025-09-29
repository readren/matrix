package readren.sequencer
package manager
package descriptors

import readren.common.CompileTime.getTypeName
import readren.sequencer.providers.RoundRobinDp


object DefaultRoundRobinDpd extends DoerProviderDescriptor[Doer](getTypeName[DefaultRoundRobinDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[Doer] = new RoundRobinDp.Impl()
}
